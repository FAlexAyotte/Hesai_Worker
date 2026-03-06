// main.cpp
//
// LiDAR recorder (Hesai XT32M2X-style UDP stream):
// - Captures raw packets to rotating PCAP (~2GiB by default)
// - Writes per-packet JSONL sidecar (seq + LiDAR timestamp + capture timestamps)
// - Writes lidar_settings_config-<session_id>.json (updated on rotation + stop)
//
// Two build modes (controlled by CMake):
//
// 1. Standalone (BUILD_ROS2=OFF, default):
//    - HTTP control API: /health, /status, /stats, /start, /stop, /restart
//    - Build: cmake -DBUILD_ROS2=OFF .. && make
//    - Run:   ./lidar_recorder --bind 127.0.0.1 --port 8080
//
// 2. ROS 2 lifecycle node (BUILD_ROS2=ON):
//    - Controlled via ROS 2 lifecycle transitions (configure/activate/deactivate)
//    - Publishes /hesai/stats at 1Hz
//    - Build: colcon build --packages-select hesai_worker
//    - Run:   ros2 run hesai_worker hesai_node --ros-args --params-file config/hesai_params.yaml

#include <pcap.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifdef BUILD_ROS2
#include <rclcpp/rclcpp.hpp>
#include <rclcpp_lifecycle/lifecycle_node.hpp>
#include <diagnostic_msgs/msg/diagnostic_status.hpp>
#include <diagnostic_msgs/msg/key_value.hpp>
#else
#include "httplib.h"
#endif

// -------------------- endian helpers --------------------
static uint16_t be16(const uint8_t* p) { return (uint16_t(p[0]) << 8) | uint16_t(p[1]); }
static uint32_t be32(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}

// -------------------- time helpers --------------------
static std::string utc_iso8601_now() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto s = time_point_cast<seconds>(now);
  auto us = duration_cast<microseconds>(now - s).count();
  std::time_t t = system_clock::to_time_t(s);
  std::tm tm{};
  gmtime_r(&t, &tm);
  std::ostringstream os;
  os << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(6) << std::setfill('0') << us << "Z";
  return os.str();
}

static std::string iso_compact_now_utc() {
  using namespace std::chrono;
  auto now = system_clock::now();
  std::time_t t = system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&t, &tm);
  std::ostringstream os;
  os << std::put_time(&tm, "%Y%m%dT%H%M%SZ");
  return os.str();
}

static std::string utc_from_epoch_us(int64_t sec, int64_t usec) {
  std::time_t t = static_cast<std::time_t>(sec);
  std::tm tm{};
  gmtime_r(&t, &tm);
  std::ostringstream os;
  os << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(6) << std::setfill('0')
     << static_cast<long long>(usec) << "Z";
  return os.str();
}

static std::string gen_session_id() {
  static std::mt19937_64 rng{std::random_device{}()};
  uint64_t r = rng();
  std::ostringstream os;
  os << iso_compact_now_utc() << "-" << std::hex << r;
  return os.str();
}

// -------------------- string / filesystem helpers --------------------
static std::string join_path(const std::string& dir, const std::string& file) {
  if (dir.empty()) return file;
  if (dir.back() == '/') return dir + file;
  return dir + "/" + file;
}

static bool ensure_dir_exists(const std::string& dir) {
  if (dir.empty()) return true;
  std::string cmd = "mkdir -p " + dir;
  return (std::system(cmd.c_str()) == 0);
}

static std::string json_escape(const std::string& s) {
  std::ostringstream os;
  for (char c : s) {
    switch (c) {
      case '\"': os << "\\\""; break;
      case '\\': os << "\\\\"; break;
      case '\n': os << "\\n"; break;
      case '\r': os << "\\r"; break;
      case '\t': os << "\\t"; break;
      default: os << c; break;
    }
  }
  return os.str();
}

// -------------------- minimal JSON field extraction (no dependency) --------------------
static std::optional<std::string> json_get_string(const std::string& body, const std::string& key) {
  auto k = "\"" + key + "\"";
  auto pos = body.find(k);
  if (pos == std::string::npos) return std::nullopt;
  pos = body.find(':', pos);
  if (pos == std::string::npos) return std::nullopt;
  pos = body.find('"', pos);
  if (pos == std::string::npos) return std::nullopt;
  auto end = body.find('"', pos + 1);
  if (end == std::string::npos) return std::nullopt;
  return body.substr(pos + 1, end - (pos + 1));
}

static std::optional<uint64_t> json_get_u64(const std::string& body, const std::string& key) {
  auto k = "\"" + key + "\"";
  auto pos = body.find(k);
  if (pos == std::string::npos) return std::nullopt;
  pos = body.find(':', pos);
  if (pos == std::string::npos) return std::nullopt;

  pos++;
  while (pos < body.size() &&
         (body[pos] == ' ' || body[pos] == '\n' || body[pos] == '\t' || body[pos] == '\r'))
    pos++;

  size_t end = pos;
  while (end < body.size() && (body[end] >= '0' && body[end] <= '9')) end++;
  if (end == pos) return std::nullopt;

  return std::stoull(body.substr(pos, end - pos));
}

#ifndef BUILD_ROS2
// -------------------- API logger (append-only JSONL, standalone mode only) --------------------
class ApiLogger {
 public:
  bool open(const std::string& path) {
    std::lock_guard<std::mutex> lk(mu_);
    path_ = path;
    out_.open(path_, std::ios::out | std::ios::app | std::ios::binary);
    return (bool)out_;
  }

  void log(const httplib::Request& req, int http_status, const std::string& response_body) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!out_) return;

    out_ << "{"
         << "\"ts\":\"" << json_escape(utc_iso8601_now()) << "\","
         << "\"remote\":\"" << json_escape(req.remote_addr) << "\","
         << "\"method\":\"" << json_escape(req.method) << "\","
         << "\"path\":\"" << json_escape(req.path) << "\","
         << "\"body\":\"" << json_escape(req.body) << "\","
         << "\"http_status\":" << http_status << ","
         << "\"response\":\"" << json_escape(response_body) << "\""
         << "}\n";
    out_.flush();
  }

 private:
  std::mutex mu_;
  std::string path_;
  std::ofstream out_;
};
#endif  // !BUILD_ROS2

// -------------------- packet parsing (no VLAN support in this version) --------------------
struct ParsedUdp {
  const uint8_t* payload = nullptr;
  size_t payload_len = 0;
  uint16_t src_port = 0;
  uint16_t dst_port = 0;
};

static std::optional<ParsedUdp> parse_udp_ipv4_ethernet(const uint8_t* pkt, size_t len) {
  if (len < 14) return std::nullopt;
  uint16_t ethertype = be16(pkt + 12);
  if (ethertype != 0x0800) return std::nullopt;  // IPv4

  const uint8_t* ip = pkt + 14;
  if (len < 14 + 20) return std::nullopt;

  uint8_t ver_ihl = ip[0];
  uint8_t ihl = (ver_ihl & 0x0F) * 4;
  if (ihl < 20) return std::nullopt;
  if (len < 14 + ihl + 8) return std::nullopt;

  if (ip[9] != 17) return std::nullopt;  // UDP

  const uint8_t* udp = ip + ihl;
  uint16_t src = be16(udp + 0);
  uint16_t dst = be16(udp + 2);
  uint16_t udplen = be16(udp + 4);
  if (udplen < 8) return std::nullopt;

  const uint8_t* payload = udp + 8;
  size_t available = len - (payload - pkt);
  size_t declared = udplen - 8;
  size_t payload_len = (declared < available) ? declared : available;

  return ParsedUdp{payload, payload_len, src, dst};
}

// -------------------- XT32M2X tail parsing for stats --------------------
struct LidarMeta {
  uint32_t udp_seq = 0;
  uint16_t motor_rpm = 0;
  uint8_t return_mode = 0;
  int year = 0, mon = 0, day = 0, hour = 0, min = 0, sec = 0;
  uint32_t usec = 0;
};

static std::optional<LidarMeta> parse_xt32_tail(const uint8_t* payload, size_t n) {
  if (n < 28) return std::nullopt;
  const uint8_t* end = payload + n;

  const uint8_t* udp_seq_p = end - 4;
  uint32_t udp_seq = be32(udp_seq_p);

  const uint8_t* factory_p = udp_seq_p - 1;
  (void)factory_p;

  const uint8_t* ts_p = factory_p - 4;
  uint32_t usec = be32(ts_p);

  const uint8_t* dt_p = ts_p - 6;
  int year = int(dt_p[0]) + 1900;
  int mon = int(dt_p[1]);
  int day = int(dt_p[2]);
  int hour = int(dt_p[3]);
  int min = int(dt_p[4]);
  int sec = int(dt_p[5]);

  const uint8_t* motor_p = dt_p - 2;
  uint16_t motor_rpm = be16(motor_p);

  const uint8_t* rm_p = motor_p - 1;
  uint8_t return_mode = rm_p[0];

  return LidarMeta{udp_seq, motor_rpm, return_mode, year, mon, day, hour, min, sec, usec};
}

static std::string lidar_time_to_string(const LidarMeta& m) {
  std::ostringstream ts;
  ts << std::setfill('0') << m.year << "-" << std::setw(2) << m.mon << "-" << std::setw(2) << m.day << "T"
     << std::setw(2) << m.hour << ":" << std::setw(2) << m.min << ":" << std::setw(2) << m.sec << "."
     << std::setw(6) << m.usec << "Z";
  return ts.str();
}

// -------------------- timestamp source selection --------------------
enum class TimestampSource { Pcap, Realtime, Tai };

static const char* ts_source_string(TimestampSource s) {
  switch (s) {
    case TimestampSource::Pcap: return "pcap";
    case TimestampSource::Realtime: return "realtime";
    case TimestampSource::Tai: return "tai";
  }
  return "pcap";
}

static TimestampSource parse_ts_source(const std::string& s) {
  if (s == "realtime") return TimestampSource::Realtime;
  if (s == "tai") return TimestampSource::Tai;
  return TimestampSource::Pcap;
}

// -------------------- recorder --------------------
enum class State { Idle, Starting, Running, Stopping };

struct Config {
  std::string iface = "enp4s0";
  std::string out_dir = ".";
  std::string filter = "udp port 2368";
  uint64_t split_bytes = 2ull * 1024 * 1024 * 1024;  // ~2 GiB

  // LiDAR identity/connection info (for settings/config file)
  std::string lidar_model = "XT32M2X";
  std::string lidar_control_ip = "192.168.10.201";
  std::string lidar_destination_ip = "";  // typically your PC IP on LiDAR NIC (e.g., 192.168.1.10)
  uint16_t lidar_data_port = 2368;

  // Timestamp source for JSONL capture timestamps
  TimestampSource timestamp_source = TimestampSource::Pcap;
};

struct Stats {
  uint64_t packets = 0;
  uint64_t bytes = 0;

  uint64_t seq_gaps = 0;
  uint32_t last_seq = 0;
  bool have_seq = false;

  double pps = 0.0;
  double bps = 0.0;

  std::string session_id;
  uint32_t part_index = 0;

  std::string last_lidar_time;
  uint16_t last_motor_rpm = 0;

  std::string last_error;
};

struct SegmentInfo {
  uint32_t part_index = 0;
  std::string pcap_path;
  std::string jsonl_path;
  std::string started_utc;
  std::string ended_utc;
  uint64_t bytes = 0;
  uint64_t packets = 0;
};

class Recorder {
 public:
  bool start(const Config& cfg) {
    std::lock_guard<std::mutex> lk(mu_);
    if (state_ == State::Running || state_ == State::Starting) return false;
    if (state_ == State::Stopping) return false;

    cfg_ = cfg;
    (void)ensure_dir_exists(cfg_.out_dir);

    state_ = State::Starting;
    stats_ = Stats{};
    stats_.session_id = gen_session_id();
    stats_.part_index = 0;
    stop_flag_.store(false);

    session_start_utc_ = utc_iso8601_now();
    session_stop_utc_.clear();
    segments_.clear();

    session_config_path_ = join_path(cfg_.out_dir, "lidar_settings_config-" + stats_.session_id + ".json");

    char errbuf[PCAP_ERRBUF_SIZE];
    handle_ = pcap_open_live(cfg_.iface.c_str(), 65535, 1, 50, errbuf);
    if (!handle_) {
      stats_.last_error = std::string("pcap_open_live failed: ") + errbuf;
      state_ = State::Idle;
      return false;
    }

    bpf_program fp{};
    if (pcap_compile(handle_, &fp, cfg_.filter.c_str(), 1, PCAP_NETMASK_UNKNOWN) != 0) {
      stats_.last_error = std::string("pcap_compile failed: ") + pcap_geterr(handle_);
      pcap_close(handle_);
      handle_ = nullptr;
      state_ = State::Idle;
      return false;
    }
    if (pcap_setfilter(handle_, &fp) != 0) {
      stats_.last_error = std::string("pcap_setfilter failed: ") + pcap_geterr(handle_);
      pcap_freecode(&fp);
      pcap_close(handle_);
      handle_ = nullptr;
      state_ = State::Idle;
      return false;
    }
    pcap_freecode(&fp);

    if (!open_new_segment_locked(/*first*/true)) {
      pcap_close(handle_);
      handle_ = nullptr;
      state_ = State::Idle;
      return false;
    }

    capture_thread_ = std::thread([this]() { capture_loop(); });
    stats_thread_ = std::thread([this]() { stats_loop(); });

    write_session_config_locked();  // initial snapshot
    state_ = State::Running;
    return true;
  }

  bool stop() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (state_ == State::Idle) return true;      // idempotent
      if (state_ == State::Stopping) return true;  // already stopping
      if (state_ == State::Starting) return false;

      state_ = State::Stopping;
      stop_flag_.store(true);
      if (handle_) pcap_breakloop(handle_);  // unblock capture loop quickly
    }

    if (capture_thread_.joinable()) capture_thread_.join();
    if (stats_thread_.joinable()) stats_thread_.join();

    std::lock_guard<std::mutex> lk(mu_);

    session_stop_utc_ = utc_iso8601_now();
    if (!segments_.empty() && segments_.back().ended_utc.empty()) {
      segments_.back().ended_utc = session_stop_utc_;
    }
    write_session_config_locked();

    close_segment_locked();

    if (handle_) {
      pcap_close(handle_);
      handle_ = nullptr;
    }
    state_ = State::Idle;
    return true;
  }

  void set_worker_start_utc(const std::string& t) {
    std::lock_guard<std::mutex> lk(mu_);
    worker_start_utc_ = t;
  }

  void set_worker_stop_utc(const std::string& t) {
    std::lock_guard<std::mutex> lk(mu_);
    worker_stop_utc_ = t;
    if (!session_config_path_.empty()) write_session_config_locked();
  }

  std::string status_json() {
    std::lock_guard<std::mutex> lk(mu_);
    std::ostringstream os;
    os << "{"
       << "\"state\":\"" << state_string_locked() << "\","
       << "\"iface\":\"" << json_escape(cfg_.iface) << "\","
       << "\"out_dir\":\"" << json_escape(cfg_.out_dir) << "\","
       << "\"filter\":\"" << json_escape(cfg_.filter) << "\","
       << "\"split_bytes\":" << cfg_.split_bytes << ","
       << "\"timestamp_source\":\"" << json_escape(ts_source_string(cfg_.timestamp_source)) << "\","
       << "\"session_id\":\"" << json_escape(stats_.session_id) << "\","
       << "\"part_index\":" << stats_.part_index << ","
       << "\"current_pcap\":\"" << json_escape(current_pcap_path_) << "\","
       << "\"current_jsonl\":\"" << json_escape(current_jsonl_path_) << "\","
       << "\"error\":\"" << json_escape(stats_.last_error) << "\""
       << "}";
    return os.str();
  }

  std::string stats_json() {
    std::lock_guard<std::mutex> lk(mu_);
    std::ostringstream os;
    os << "{"
       << "\"packets\":" << stats_.packets << ","
       << "\"bytes\":" << stats_.bytes << ","
       << "\"pps\":" << stats_.pps << ","
       << "\"bps\":" << stats_.bps << ","
       << "\"seq_gaps\":" << stats_.seq_gaps << ","
       << "\"last_seq\":" << stats_.last_seq << ","
       << "\"last_lidar_time\":\"" << json_escape(stats_.last_lidar_time) << "\","
       << "\"motor_rpm\":" << stats_.last_motor_rpm << ","
       << "\"session_id\":\"" << json_escape(stats_.session_id) << "\","
       << "\"part_index\":" << stats_.part_index
       << "}";
    return os.str();
  }

  Stats get_stats() const {
    std::lock_guard<std::mutex> lk(mu_);
    return stats_;
  }

  std::string get_state_string() const {
    std::lock_guard<std::mutex> lk(mu_);
    return state_string_locked();
  }

 private:
  std::string state_string_locked() const {
    switch (state_) {
      case State::Idle: return "idle";
      case State::Starting: return "starting";
      case State::Running: return "running";
      case State::Stopping: return "stopping";
    }
    return "unknown";
  }

  std::string make_segment_basename_locked(uint32_t part_idx) const {
    std::ostringstream os;
    os << "xt32m2x-" << stats_.session_id << "-part" << std::setw(4) << std::setfill('0') << part_idx;
    return os.str();
  }

  bool open_new_segment_locked(bool first) {
    close_segment_locked();

    const auto base = make_segment_basename_locked(stats_.part_index);
    current_pcap_path_ = join_path(cfg_.out_dir, base + ".pcap");
    current_jsonl_path_ = join_path(cfg_.out_dir, base + ".jsonl");

    dumper_ = pcap_dump_open(handle_, current_pcap_path_.c_str());
    if (!dumper_) {
      stats_.last_error = std::string("pcap_dump_open failed: ") + pcap_geterr(handle_);
      return false;
    }

    jsonl_.open(current_jsonl_path_, std::ios::out | std::ios::binary);
    if (!jsonl_) {
      stats_.last_error = "Failed to open JSONL sidecar: " + current_jsonl_path_;
      pcap_dump_close(dumper_);
      dumper_ = nullptr;
      return false;
    }

    if (first) {
      jsonl_ << "{"
             << "\"type\":\"session_start\","
             << "\"session_id\":\"" << json_escape(stats_.session_id) << "\","
             << "\"split_bytes\":" << cfg_.split_bytes << ","
             << "\"filter\":\"" << json_escape(cfg_.filter) << "\","
             << "\"iface\":\"" << json_escape(cfg_.iface) << "\","
             << "\"timestamp_source\":\"" << json_escape(ts_source_string(cfg_.timestamp_source)) << "\""
             << "}\n";
      jsonl_.flush();
    }

    SegmentInfo seg;
    seg.part_index = stats_.part_index;
    seg.pcap_path = current_pcap_path_;
    seg.jsonl_path = current_jsonl_path_;
    seg.started_utc = utc_iso8601_now();
    segments_.push_back(seg);

    write_session_config_locked();
    return true;
  }

  void close_segment_locked() {
    if (jsonl_.is_open()) {
      jsonl_.flush();
      jsonl_.close();
    }
    if (dumper_) {
      pcap_dump_flush(dumper_);
      pcap_dump_close(dumper_);
      dumper_ = nullptr;
    }
  }

  void rotate_if_needed_locked() {
    if (!dumper_) return;
    long long sz = pcap_dump_ftell(dumper_);
    if (sz < 0) return;

    if (static_cast<uint64_t>(sz) >= cfg_.split_bytes) {
      if (!segments_.empty() && segments_.back().ended_utc.empty()) {
        segments_.back().ended_utc = utc_iso8601_now();
      }
      stats_.part_index += 1;
      (void)open_new_segment_locked(/*first*/false);
    }
  }

  // Returns capture timestamp based on configured source.
  // Always also records pcap timestamp separately in JSONL for debugging.
  static void compute_capture_ts(TimestampSource src,
                                 const pcap_pkthdr* header,
                                 int64_t* out_sec,
                                 int64_t* out_usec) {
    if (src == TimestampSource::Pcap) {
      *out_sec = header->ts.tv_sec;
      *out_usec = header->ts.tv_usec;
      return;
    }

    timespec ts{};
    clockid_t clk = (src == TimestampSource::Realtime) ? CLOCK_REALTIME : CLOCK_TAI;
    if (clock_gettime(clk, &ts) != 0) {
      // Fallback to pcap if clock_gettime fails
      *out_sec = header->ts.tv_sec;
      *out_usec = header->ts.tv_usec;
      return;
    }
    *out_sec = ts.tv_sec;
    *out_usec = ts.tv_nsec / 1000;
  }

  void capture_loop() {
    while (!stop_flag_.load()) {
      pcap_pkthdr* header = nullptr;
      const u_char* data = nullptr;

      int rc = pcap_next_ex(handle_, &header, &data);
      if (rc == 0) continue;           // timeout
      if (rc == -1 || rc == -2) break; // error or EOF/breakloop

      // Dump raw frame to PCAP
      {
        std::lock_guard<std::mutex> lk(mu_);
        if (dumper_) pcap_dump((u_char*)dumper_, header, data);
      }

      auto udp = parse_udp_ipv4_ethernet(data, header->caplen);
      std::optional<LidarMeta> meta;
      if (udp && udp->payload && udp->payload_len >= 28) {
        meta = parse_xt32_tail(udp->payload, udp->payload_len);
      }

      // Capture timestamp selection
      int64_t cap_sec = 0, cap_usec = 0;
      compute_capture_ts(cfg_.timestamp_source, header, &cap_sec, &cap_usec);

      {
        std::lock_guard<std::mutex> lk(mu_);

        stats_.packets += 1;
        stats_.bytes += header->caplen;

        if (!segments_.empty()) {
          segments_.back().packets += 1;
          segments_.back().bytes += header->caplen;
        }

        uint32_t seq = 0;
        std::string lidar_ts;
        uint16_t motor = 0;
        uint8_t rm = 0;

        if (meta) {
          seq = meta->udp_seq;
          motor = meta->motor_rpm;
          rm = meta->return_mode;
          lidar_ts = lidar_time_to_string(*meta);

          if (stats_.have_seq) {
            uint32_t expected = stats_.last_seq + 1;
            if (seq != expected) {
              stats_.seq_gaps += (seq > expected) ? (seq - expected) : 1;
            }
          } else {
            stats_.have_seq = true;
          }

          stats_.last_seq = seq;
          stats_.last_lidar_time = lidar_ts;
          stats_.last_motor_rpm = motor;
        }

        // JSONL packet line
        if (jsonl_.is_open()) {
          jsonl_ << "{"
                 << "\"type\":\"pkt\","
                 << "\"session_id\":\"" << json_escape(stats_.session_id) << "\","
                 << "\"part_index\":" << stats_.part_index << ","
                 << "\"caplen\":" << header->caplen << ","
                 << "\"wirelen\":" << header->len << ",";

          // Always record pcap timestamp for debugging
          jsonl_ << "\"pcap_sec\":" << header->ts.tv_sec << ","
                 << "\"pcap_usec\":" << header->ts.tv_usec << ",";

          // Selected capture timestamp (PTP-aligned if using realtime and system clock is disciplined)
          jsonl_ << "\"capture_ts_source\":\"" << json_escape(ts_source_string(cfg_.timestamp_source)) << "\","
                 << "\"capture_sec\":" << cap_sec << ","
                 << "\"capture_usec\":" << cap_usec << ",";

          // Human-friendly UTC string when the epoch is meaningful (pcap/realtime).
          if (cfg_.timestamp_source == TimestampSource::Pcap || cfg_.timestamp_source == TimestampSource::Realtime) {
            jsonl_ << "\"capture_time_utc\":\"" << json_escape(utc_from_epoch_us(cap_sec, cap_usec)) << "\",";
          } else {
            // For CLOCK_TAI, keep numeric epoch fields; conversion to UTC requires knowing the current TAI-UTC offset.
            jsonl_ << "\"capture_time_utc\":null,";
          }

          if (meta) {
            jsonl_ << "\"udp_seq\":" << seq << ","
                   << "\"lidar_time\":\"" << json_escape(lidar_ts) << "\","
                   << "\"motor_rpm\":" << motor << ","
                   << "\"return_mode\":" << int(rm) << ",";
          } else {
            jsonl_ << "\"udp_seq\":null,\"lidar_time\":null,\"motor_rpm\":null,\"return_mode\":null,";
          }

          jsonl_ << "\"note\":\"xt32_tail_parse_no_vlan\""
                 << "}\n";
        }

        // Flush + rotate
        if ((stats_.packets % 2000) == 0) {
          if (dumper_) pcap_dump_flush(dumper_);
          if (jsonl_.is_open()) jsonl_.flush();
          rotate_if_needed_locked();
        } else {
          rotate_if_needed_locked();
        }
      }
    }
  }

  void stats_loop() {
    using clock = std::chrono::steady_clock;
    auto t0 = clock::now();
    uint64_t p0 = 0, b0 = 0;

    while (!stop_flag_.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      std::lock_guard<std::mutex> lk(mu_);
      auto t1 = clock::now();
      double dt = std::chrono::duration<double>(t1 - t0).count();
      if (dt > 0) {
        stats_.pps = (stats_.packets - p0) / dt;
        stats_.bps = (stats_.bytes - b0) / dt;
      }
      t0 = t1;
      p0 = stats_.packets;
      b0 = stats_.bytes;
    }
  }

  void write_session_config_locked() {
    if (session_config_path_.empty()) return;

    std::ofstream f(session_config_path_, std::ios::out | std::ios::binary);
    if (!f) return;

    f << "{"
      << "\"type\":\"lidar_settings_config\","
      << "\"session_id\":\"" << json_escape(stats_.session_id) << "\","
      << "\"worker_start_utc\":\"" << json_escape(worker_start_utc_) << "\","
      << "\"worker_stop_utc\":\"" << json_escape(worker_stop_utc_) << "\","
      << "\"recording_start_utc\":\"" << json_escape(session_start_utc_) << "\","
      << "\"recording_stop_utc\":\"" << json_escape(session_stop_utc_) << "\","
      << "\"lidar\":{"
      << "\"model\":\"" << json_escape(cfg_.lidar_model) << "\","
      << "\"control_ip\":\"" << json_escape(cfg_.lidar_control_ip) << "\","
      << "\"destination_ip\":\"" << json_escape(cfg_.lidar_destination_ip) << "\","
      << "\"data_port\":" << cfg_.lidar_data_port
      << "},"
      << "\"capture\":{"
      << "\"iface\":\"" << json_escape(cfg_.iface) << "\","
      << "\"filter\":\"" << json_escape(cfg_.filter) << "\","
      << "\"split_bytes\":" << cfg_.split_bytes << ","
      << "\"timestamp_source\":\"" << json_escape(ts_source_string(cfg_.timestamp_source)) << "\""
      << "},"
      << "\"totals\":{"
      << "\"packets\":" << stats_.packets << ","
      << "\"bytes\":" << stats_.bytes << ","
      << "\"seq_gaps\":" << stats_.seq_gaps
      << "},"
      << "\"segments\":[";

    for (size_t i = 0; i < segments_.size(); i++) {
      const auto& s = segments_[i];
      if (i) f << ",";
      f << "{"
        << "\"part_index\":" << s.part_index << ","
        << "\"pcap_path\":\"" << json_escape(s.pcap_path) << "\","
        << "\"jsonl_path\":\"" << json_escape(s.jsonl_path) << "\","
        << "\"started_utc\":\"" << json_escape(s.started_utc) << "\","
        << "\"ended_utc\":\"" << json_escape(s.ended_utc) << "\","
        << "\"packets\":" << s.packets << ","
        << "\"bytes\":" << s.bytes
        << "}";
    }

    f << "]"
      << "}\n";
  }

 private:
  mutable std::mutex mu_;
  State state_ = State::Idle;

  Config cfg_;
  Stats stats_;

  pcap_t* handle_ = nullptr;
  pcap_dumper_t* dumper_ = nullptr;

  std::ofstream jsonl_;
  std::string current_pcap_path_;
  std::string current_jsonl_path_;

  // session + worker info
  std::string session_config_path_;
  std::string worker_start_utc_;
  std::string worker_stop_utc_;
  std::string session_start_utc_;
  std::string session_stop_utc_;
  std::vector<SegmentInfo> segments_;

  std::thread capture_thread_;
  std::thread stats_thread_;
  std::atomic<bool> stop_flag_{false};
};

#ifdef BUILD_ROS2
// ==================== ROS 2 lifecycle node ====================

class HesaiNode : public rclcpp_lifecycle::LifecycleNode {
 public:
  explicit HesaiNode(const rclcpp::NodeOptions& options = rclcpp::NodeOptions())
      : rclcpp_lifecycle::LifecycleNode("hesai_node", options) {
    // Declare parameters with defaults
    this->declare_parameter<std::string>("iface", "enp0s31f6");
    this->declare_parameter<std::string>("out_dir", "/tmp/hesai");
    this->declare_parameter<std::string>("filter", "udp port 2368");
    this->declare_parameter<int64_t>("split_bytes", 2147483648LL);
    this->declare_parameter<std::string>("timestamp_source", "realtime");
    this->declare_parameter<std::string>("lidar_model", "XT32M2X");
    this->declare_parameter<std::string>("lidar_control_ip", "192.168.10.201");
    this->declare_parameter<std::string>("lidar_destination_ip", "");
    this->declare_parameter<int>("lidar_data_port", 2368);
  }

  using CallbackReturn = rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn;

  CallbackReturn on_configure(const rclcpp_lifecycle::State&) override {
    RCLCPP_INFO(get_logger(), "Configuring...");

    cfg_.iface = this->get_parameter("iface").as_string();
    cfg_.out_dir = this->get_parameter("out_dir").as_string();
    cfg_.filter = this->get_parameter("filter").as_string();
    cfg_.split_bytes = static_cast<uint64_t>(this->get_parameter("split_bytes").as_int());
    cfg_.timestamp_source = parse_ts_source(this->get_parameter("timestamp_source").as_string());
    cfg_.lidar_model = this->get_parameter("lidar_model").as_string();
    cfg_.lidar_control_ip = this->get_parameter("lidar_control_ip").as_string();
    cfg_.lidar_destination_ip = this->get_parameter("lidar_destination_ip").as_string();
    cfg_.lidar_data_port = static_cast<uint16_t>(this->get_parameter("lidar_data_port").as_int());

    stats_pub_ = this->create_publisher<diagnostic_msgs::msg::DiagnosticStatus>(
        "~/stats", rclcpp::QoS(10));

    RCLCPP_INFO(get_logger(), "Configured: iface=%s out_dir=%s filter='%s' split=%lu ts=%s",
                cfg_.iface.c_str(), cfg_.out_dir.c_str(), cfg_.filter.c_str(),
                cfg_.split_bytes, ts_source_string(cfg_.timestamp_source));
    return CallbackReturn::SUCCESS;
  }

  CallbackReturn on_activate(const rclcpp_lifecycle::State&) override {
    RCLCPP_INFO(get_logger(), "Activating — starting capture...");

    recorder_.set_worker_start_utc(utc_iso8601_now());

    if (!recorder_.start(cfg_)) {
      RCLCPP_ERROR(get_logger(), "Failed to start recorder");
      return CallbackReturn::FAILURE;
    }

    stats_pub_->on_activate();

    // 1Hz stats publisher timer
    stats_timer_ = this->create_wall_timer(
        std::chrono::seconds(1),
        [this]() { publish_stats(); });

    RCLCPP_INFO(get_logger(), "Active — capturing packets");
    return CallbackReturn::SUCCESS;
  }

  CallbackReturn on_deactivate(const rclcpp_lifecycle::State&) override {
    RCLCPP_INFO(get_logger(), "Deactivating — stopping capture...");

    stats_timer_.reset();
    recorder_.stop();
    recorder_.set_worker_stop_utc(utc_iso8601_now());
    stats_pub_->on_deactivate();

    RCLCPP_INFO(get_logger(), "Deactivated — capture stopped");
    return CallbackReturn::SUCCESS;
  }

  CallbackReturn on_cleanup(const rclcpp_lifecycle::State&) override {
    RCLCPP_INFO(get_logger(), "Cleaning up...");
    stats_pub_.reset();
    cfg_ = Config{};
    return CallbackReturn::SUCCESS;
  }

  CallbackReturn on_shutdown(const rclcpp_lifecycle::State& state) override {
    RCLCPP_INFO(get_logger(), "Shutting down from state '%s'...", state.label().c_str());
    stats_timer_.reset();
    recorder_.stop();
    recorder_.set_worker_stop_utc(utc_iso8601_now());
    stats_pub_.reset();
    return CallbackReturn::SUCCESS;
  }

  CallbackReturn on_error(const rclcpp_lifecycle::State& state) override {
    RCLCPP_ERROR(get_logger(), "Error in state '%s' — attempting recovery...", state.label().c_str());
    stats_timer_.reset();
    recorder_.stop();
    // Return SUCCESS to transition to Unconfigured (allows retry)
    return CallbackReturn::SUCCESS;
  }

 private:
  void publish_stats() {
    if (!stats_pub_->is_activated()) return;

    auto stats = recorder_.get_stats();
    auto state_str = recorder_.get_state_string();

    diagnostic_msgs::msg::DiagnosticStatus msg;
    msg.name = "hesai_recorder";
    msg.hardware_id = cfg_.lidar_model;

    // Set level based on recorder state
    if (state_str == "running") {
      msg.level = diagnostic_msgs::msg::DiagnosticStatus::OK;
      msg.message = "Capturing";
    } else if (state_str == "idle") {
      msg.level = diagnostic_msgs::msg::DiagnosticStatus::WARN;
      msg.message = "Idle";
    } else {
      msg.level = diagnostic_msgs::msg::DiagnosticStatus::STALE;
      msg.message = state_str;
    }

    auto kv = [](const std::string& k, const std::string& v) {
      diagnostic_msgs::msg::KeyValue pair;
      pair.key = k;
      pair.value = v;
      return pair;
    };

    msg.values.push_back(kv("state", state_str));
    msg.values.push_back(kv("pps", std::to_string(stats.pps)));
    msg.values.push_back(kv("bps", std::to_string(stats.bps)));
    msg.values.push_back(kv("packets", std::to_string(stats.packets)));
    msg.values.push_back(kv("bytes", std::to_string(stats.bytes)));
    msg.values.push_back(kv("seq_gaps", std::to_string(stats.seq_gaps)));
    msg.values.push_back(kv("motor_rpm", std::to_string(stats.last_motor_rpm)));
    msg.values.push_back(kv("session_id", stats.session_id));
    msg.values.push_back(kv("part_index", std::to_string(stats.part_index)));

    if (!stats.last_lidar_time.empty())
      msg.values.push_back(kv("lidar_time", stats.last_lidar_time));
    if (!stats.last_error.empty())
      msg.values.push_back(kv("error", stats.last_error));

    stats_pub_->publish(msg);
  }

  Recorder recorder_;
  Config cfg_;
  rclcpp_lifecycle::LifecyclePublisher<diagnostic_msgs::msg::DiagnosticStatus>::SharedPtr stats_pub_;
  rclcpp::TimerBase::SharedPtr stats_timer_;
};

int main(int argc, char** argv) {
  rclcpp::init(argc, argv);
  auto node = std::make_shared<HesaiNode>();
  rclcpp::spin(node->get_node_base_interface());
  rclcpp::shutdown();
  return 0;
}

#else
// ==================== Standalone HTTP mode ====================

static void add_common_headers(httplib::Response& res) {
  res.set_header("Content-Type", "application/json");
  res.set_header("Cache-Control", "no-store");
}

static httplib::Server* g_server = nullptr;

static void on_signal(int) {
  if (g_server) g_server->stop();
}

static void write_worker_lifecycle(const std::string& path,
                                  const std::string& bind,
                                  int port,
                                  const std::string& start_utc,
                                  const std::string& stop_utc) {
  std::ofstream f(path, std::ios::out | std::ios::binary);
  if (!f) return;
  f << "{"
    << "\"type\":\"worker_lifecycle\","
    << "\"bind\":\"" << json_escape(bind) << "\","
    << "\"port\":" << port << ","
    << "\"worker_start_utc\":\"" << json_escape(start_utc) << "\","
    << "\"worker_stop_utc\":\"" << json_escape(stop_utc) << "\""
    << "}\n";
}

// Shared config parsing for /start and /restart
static Config parse_config_from_body(const std::string& body) {
  Config cfg;

  if (auto v = json_get_string(body, "iface")) cfg.iface = *v;
  if (auto v = json_get_string(body, "out_dir")) cfg.out_dir = *v;
  if (auto v = json_get_string(body, "filter")) cfg.filter = *v;
  if (auto v = json_get_u64(body, "split_bytes")) cfg.split_bytes = *v;

  if (auto v = json_get_string(body, "lidar_model")) cfg.lidar_model = *v;
  if (auto v = json_get_string(body, "lidar_control_ip")) cfg.lidar_control_ip = *v;
  if (auto v = json_get_string(body, "lidar_destination_ip")) cfg.lidar_destination_ip = *v;
  if (auto v = json_get_u64(body, "lidar_data_port")) cfg.lidar_data_port = static_cast<uint16_t>(*v);

  if (auto v = json_get_string(body, "timestamp_source")) cfg.timestamp_source = parse_ts_source(*v);

  return cfg;
}

int main(int argc, char** argv) {
  std::string bind = "127.0.0.1";
  int port = 8080;
  std::string state_dir = ".";  // worker_lifecycle.json + api_commands.jsonl go here

  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "--bind" && i + 1 < argc) bind = argv[++i];
    else if (a == "--port" && i + 1 < argc) port = std::stoi(argv[++i]);
    else if (a == "--state_dir" && i + 1 < argc) state_dir = argv[++i];
  }

  (void)ensure_dir_exists(state_dir);

  Recorder recorder;
  httplib::Server svr;
  g_server = &svr;

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  const std::string worker_start = utc_iso8601_now();
  const std::string lifecycle_path = join_path(state_dir, "worker_lifecycle.json");
  write_worker_lifecycle(lifecycle_path, bind, port, worker_start, "");
  recorder.set_worker_start_utc(worker_start);

  ApiLogger api_logger;
  api_logger.open(join_path(state_dir, "api_commands.jsonl"));

  svr.Get("/health", [&](const httplib::Request& req, httplib::Response& res) {
    add_common_headers(res);
    res.body = "{\"ok\":true}";
  });

  svr.Get("/status", [&](const httplib::Request& req, httplib::Response& res) {
    add_common_headers(res);
    res.body = recorder.status_json();
  });

  svr.Get("/stats", [&](const httplib::Request& req, httplib::Response& res) {
    add_common_headers(res);
    res.body = recorder.stats_json();
  });

  svr.Post("/start", [&](const httplib::Request& req, httplib::Response& res) {
    add_common_headers(res);

    Config cfg = parse_config_from_body(req.body);

    bool ok = recorder.start(cfg);
    if (!ok) {
      res.status = 409;
      res.body = std::string("{\"ok\":false,\"status\":") + recorder.status_json() + "}";
      api_logger.log(req, res.status, res.body);
      return;
    }

    res.body = std::string("{\"ok\":true,\"status\":") + recorder.status_json() + "}";
    api_logger.log(req, 200, res.body);
  });

  svr.Post("/stop", [&](const httplib::Request& req, httplib::Response& res) {
    add_common_headers(res);
    bool ok = recorder.stop();
    res.body = std::string("{\"ok\":") + (ok ? "true" : "false") + ",\"status\":" + recorder.status_json() + "}";
    api_logger.log(req, 200, res.body);
  });

  svr.Post("/restart", [&](const httplib::Request& req, httplib::Response& res) {
    add_common_headers(res);

    (void)recorder.stop();

    Config cfg = parse_config_from_body(req.body);

    bool ok = recorder.start(cfg);
    if (!ok) {
      res.status = 409;
      res.body = std::string("{\"ok\":false,\"status\":") + recorder.status_json() + "}";
      api_logger.log(req, res.status, res.body);
      return;
    }

    res.body = std::string("{\"ok\":true,\"status\":") + recorder.status_json() + "}";
    api_logger.log(req, 200, res.body);
  });

  std::cout << "HTTP control listening on http://" << bind << ":" << port << "\n";
  std::cout << "Try: curl http://" << bind << ":" << port << "/status\n";

  svr.listen(bind.c_str(), port);

  // graceful shutdown
  recorder.stop();
  const std::string worker_stop = utc_iso8601_now();
  recorder.set_worker_stop_utc(worker_stop);
  write_worker_lifecycle(lifecycle_path, bind, port, worker_start, worker_stop);

  return 0;
}
#endif  // BUILD_ROS2
