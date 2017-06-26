def create_topic!(topic, partitions:, replication_factor:)
  success = system(
    "docker exec -it kc1 kafka-topics --create --topic #{topic} " \
    "--partitions #{partitions} --replication-factor #{replication_factor} " \
    "--zookeeper \"zoo1,zoo2,zoo3\""
  )
  raise "Error creating topic #{topic}" if !success
end

def delete_topic!(topic)
  success = system(
    "docker exec -it kc1 kafka-topics --delete --topic #{topic} --if-exists " \
    "--zookeeper \"zoo1,zoo2,zoo3\""
  )
  raise "Error deleting topic #{topic}" if !success
end
