package sd2526.trab.impl.replication;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.utils.IP;

public class KafkaReplicationManager {
	private static final Logger Log = Logger.getLogger(KafkaReplicationManager.class.getName());

	private static final String DEFAULT_BOOTSTRAP = "kafka:9092";
	private static final long APPLY_TIMEOUT_MS = 15000;
	private static final long READ_WAIT_TIMEOUT_MS = 10000;

	private final String bootstrap;
	private final String topic;
	private final ReplicatedMessagesService service;
	private final ObjectMapper json = new ObjectMapper();
	private final ConcurrentHashMap<String, CompletableFuture<OperationResult>> pending = new ConcurrentHashMap<>();
	private final AtomicLong lastApplied = new AtomicLong(-1);
	private final Object progressLock = new Object();

	private volatile boolean started;
	private KafkaProducer<String, String> producer;

	public KafkaReplicationManager(String[] args, ReplicatedMessagesService service) {
		this.bootstrap = args != null && args.length > 0 && !args[0].isBlank() ? args[0] : DEFAULT_BOOTSTRAP;
		this.topic = "sd2526-messages-" + IP.domain();
		this.service = service;
	}

	public synchronized void start() {
		if (started)
			return;
		this.producer = new KafkaProducer<>(producerProperties());
		startConsumer();
		started = true;
	}

	public long currentVersion() {
		return lastApplied.get();
	}

	public Result<Void> waitForRequestedVersion() {
		var required = VersionHeaderHandler.requestVersion();
		if (required == null || required < 0 || lastApplied.get() >= required)
			return Result.ok();

		var deadline = System.currentTimeMillis() + READ_WAIT_TIMEOUT_MS;
		synchronized (progressLock) {
			while (lastApplied.get() < required && System.currentTimeMillis() < deadline) {
				try {
					progressLock.wait(100);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return Result.error(ErrorCode.TIMEOUT);
				}
			}
		}
		return lastApplied.get() >= required ? Result.ok() : Result.error(ErrorCode.TIMEOUT);
	}

	Result<String> replicateString(ReplicatedOperation op) {
		return replicate(op).toStringResult();
	}

	Result<Void> replicateVoid(ReplicatedOperation op) {
		return replicate(op).toVoidResult();
	}

	OperationResult replicateOperation(ReplicatedOperation op) {
		return replicate(op);
	}

	private OperationResult replicate(ReplicatedOperation op) {
		start();
		var future = new CompletableFuture<OperationResult>();
		pending.put(op.getOpId(), future);
		try {
			var payload = json.writeValueAsString(op);
			producer.send(new ProducerRecord<>(topic, op.getOpId(), payload)).get(APPLY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			return future.get(APPLY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			Log.warning("Failed to replicate operation " + op.getType() + ": " + e.getMessage());
			return OperationResult.error(ErrorCode.TIMEOUT);
		} finally {
			pending.remove(op.getOpId());
		}
	}

	private void startConsumer() {
		var consumer = new KafkaConsumer<String, String>(consumerProperties());
		consumer.subscribe(java.util.List.of(topic));

		var thread = new Thread(() -> {
			while (true) {
				try {
					var records = consumer.poll(Duration.ofMillis(250));
					for (var record : records) {
						var op = json.readValue(record.value(), ReplicatedOperation.class);
						op.setOffset(record.offset());
						var result = service.apply(op);
						result.setVersion(record.offset());
						lastApplied.updateAndGet(previous -> Math.max(previous, record.offset()));
						synchronized (progressLock) {
							progressLock.notifyAll();
						}
						var future = pending.get(op.getOpId());
						if (future != null)
							future.complete(result);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, "messages-kafka-consumer-" + IP.hostname());
		thread.setDaemon(true);
		thread.start();
	}

	private Properties producerProperties() {
		var props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
		props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
		return props;
	}

	private Properties consumerProperties() {
		var props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "sd2526-" + IP.hostname() + "-" + UUID.randomUUID());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		return props;
	}
}
