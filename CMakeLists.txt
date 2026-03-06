cmake_minimum_required(VERSION 3.16)
project(hesai_worker CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(BUILD_ROS2 "Build as ROS 2 lifecycle node instead of standalone HTTP server" OFF)

if(BUILD_ROS2)
  find_package(ament_cmake REQUIRED)
  find_package(rclcpp REQUIRED)
  find_package(rclcpp_lifecycle REQUIRED)
  find_package(diagnostic_msgs REQUIRED)

  add_executable(hesai_node src/main.cpp)
  target_compile_definitions(hesai_node PRIVATE BUILD_ROS2)
  target_include_directories(hesai_node PRIVATE src)
  target_link_libraries(hesai_node PRIVATE
    pcap pthread
    rclcpp::rclcpp
    rclcpp_lifecycle::rclcpp_lifecycle
    ${diagnostic_msgs_TARGETS}
  )



  install(TARGETS hesai_node
    DESTINATION lib/${PROJECT_NAME})
  install(DIRECTORY config/
    DESTINATION share/${PROJECT_NAME}/config)

  ament_package()
else()
  # Standalone build (original HTTP server mode) — unchanged
  add_executable(lidar_recorder src/main.cpp)
  target_include_directories(lidar_recorder PRIVATE src)
  target_link_libraries(lidar_recorder PRIVATE pcap pthread)
endif()
