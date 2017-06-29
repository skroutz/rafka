def rand_id(type=nil)
  SecureRandom.hex(4).prepend(
    case type
    when :cgroup then "G"
    when :cons then "C"
    else ""
    end
  )
end

def new_consumer(topic, group=rand_id(:cgroup), id=rand_id(:cons))
  Rafka::Consumer.new(CLIENT_DEFAULTS.merge(topic: topic, group: group, id: id))
end

# @return [nil, Rafka::Message]
def consume_with_retry(consumer, timeout: CONSUME_TIMEOUT, retries: CONSUME_RETRIES)
  res = nil
  retries.times do
    res = consumer.consume(timeout)
    return res if res
  end
  res
end

def produce_and_flush!(prod, topic, msg)
  prod.produce(topic, msg)
  flush!(prod)
end

# @raise [] if there are still unflushed messages
def flush!(prod)
  unflushed = prod.flush(FLUSH_TIMEOUT)
  flunk("#{unflushed} unflushed messages remained") if unflushed > 0
end

def start_consumer!(cons)
  cons.consume(1)
end

# Creates a new topic and optionally a consumer to consume from it.
def with_new_topic(topic: "rafka-test-#{Time.now.to_i}-#{rand_id}",
               partitions: 4, replication_factor: 2,
               consumer: false)
  create_kafka_topic!(topic, partitions, replication_factor)
  $topics << topic

  consumer = consumer ? new_consumer(topic) : nil

  yield topic, consumer
end

def create_kafka_topic!(topic, part, repl_factor)
  out = `docker exec -it kc2 kafka-topics --create --topic #{topic} \
         --partitions #{part} --replication-factor #{repl_factor} \
         --zookeeper \"zoo1,zoo2,zoo3\"`

  raise "Error creating topic #{topic}: #{out}" if !$?.success?
end

def delete_kafka_topic!(topic)
  out = `docker exec -it kc1 kafka-topics --delete --topic #{topic} --if-exists \
         --zookeeper \"zoo1,zoo2,zoo3\"`

  raise "Error deleting topic #{topic}: #{out}" if !$?.success?
end


# ASSERTIONS
def assert_rafka_msg(msg)
  assert_kind_of Rafka::Message, msg
end

def assert_rafka_msg_equal(exp, act, msg=nil)
  assert_rafka_msg(act)
  assert_equal exp, act.value
end
