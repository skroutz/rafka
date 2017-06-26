# @return [nil, Rafka::Message]
def consume_with_retry(consumer)
  res = nil
  CONSUME_RETRIES.times do
    res = consumer.consume(2)
    return res if res
  end
  res
end

def produce_and_flush!(prod, topic, msg)
  prod.produce(topic, msg)
  flush!(prod)
end

def flush!(prod)
  unflushed = prod.flush(FLUSH_TIMEOUT)
  flunk("#{unflushed} unflushed messages remained") if unflushed > 0
end

def start_consumer!(cons)
  cons.consume(1)
end

def create_kafka_topic!(topic, partitions:, replication_factor:)
  success = system(
    "docker exec -it kc1 kafka-topics --create --topic #{topic} " \
    "--partitions #{partitions} --replication-factor #{replication_factor} " \
    "--zookeeper \"zoo1,zoo2,zoo3\""
  )
  raise "Error creating topic #{topic}" if !success
end

def delete_kafka_topic!(topic)
  success = system(
    "docker exec -it kc1 kafka-topics --delete --topic #{topic} --if-exists " \
    "--zookeeper \"zoo1,zoo2,zoo3\""
  )
  raise "Error deleting topic #{topic}" if !success
end

# assertions
def assert_rafka_msg(msg)
  assert_kind_of Rafka::Message, msg
end

def assert_rafka_msg_equal(exp, act, msg=nil)
  assert_rafka_msg(act)
  assert_equal exp, act.value
end
