package io.resx.core;

import io.resx.core.event.DistributedEvent;
import io.resx.core.event.FailedEvent;
import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.MultiMap;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import io.vertx.rxjava.ext.mongo.MongoClient;
import rx.Observable;

import java.util.LinkedList;
import java.util.List;

import static io.resx.core.Constants.ERROR_HEADER;
import static io.resx.core.Constants.HEADER_TRUE;


public class MongoEventStore implements EventStore
{
	private final EventBus eventBus;
	private final MongoClient mongoClient;

	public MongoEventStore(Vertx vertx, EventBus eventBus) {
		this.eventBus = eventBus;
		final JsonObject config = new JsonObject();
		String mongoPort = System.getenv("MONGOPORT");
		mongoClient = MongoClient.createShared(vertx, config, "mongodb://localhost:3001");
	}

	@Override public <T extends SourcedEvent> Observable<T> publish(T message, Class<T> clazz) {
		return publish(message.getAddress(), message, clazz);
	}

	@Override public <T extends DistributedEvent, R> Observable<R> publish(T message, Class<R> clazz) {
		return publish(message.getAddress(), message, clazz);
	}

	@Override public <T extends DistributedEvent, R> Observable<R> publish(String address, T message, Class<R> clazz) {
		return eventBus.sendObservable(address, Json.encode(message))
				.flatMap(objectMessage -> {
					String messageBody = (String) objectMessage.body();
					if(!hasSendError(objectMessage)) {
						//noinspection unchecked
						R entity = clazz.equals(String.class)
								? (R)messageBody
								: Json.decodeValue(messageBody, clazz);
						return Observable.just(entity);
					}
					return Observable.error(new RuntimeException(messageBody));
				});
	}

	@Override public <T extends SourcedEvent> Observable<T> publish(String address, T message, Class<T> clazz) {
		eventBus.publish(address, Json.encode(message));
		PersistableEvent<T> tPersistableEvent = new PersistableEvent<>(clazz, Json.encode(message));
		return insert(tPersistableEvent).flatMap(tPersistableEvent1 -> Observable.just(message));
	}

	@Override public <T extends FailedEvent> Observable<T> publish(String address, T message) {
		eventBus.publish(address, Json.encode(message));
		PersistableEvent<? extends FailedEvent> persistableEvent = new PersistableEvent<>(message.getClass(), Json.encode(message));
		return insert(persistableEvent).flatMap(tPersistableEvent1 -> Observable.just(message));
	}

	@Override public <T extends Aggregate> Observable<T> publish(String address, T message) {
		eventBus.publish(address, Json.encode(message));
		return Observable.just(message);
	}

	private boolean hasSendError(Message<Object> messageAsyncResult) {
		MultiMap headers = messageAsyncResult.headers();
		return HEADER_TRUE.equals(headers.get(ERROR_HEADER));
	}

	@Override public <T extends DistributedEvent> MessageConsumer<String> consumer(Class<T> event, Handler<Message<String>> handler) {
		try
		{
			return eventBus.consumer(event.newInstance().getAddress(), handler);
		}
		catch (InstantiationException | IllegalAccessException ignored) { }
		return null;
	}

	@Override public <T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass) {
		try
		{
			T aggregate;
			aggregate = aggregateClass.newInstance();

			JsonObject query = new JsonObject();
			query.put("payload.id", id);

			return getPersistableEventList(query).flatMap(persistableEvents -> {
				persistableEvents.stream()
						.filter(event -> !(FailedEvent.class.isAssignableFrom(event.getClazz())))
						.forEach(event -> {
							try {
								final Class<? extends SourcedEvent> clazz = event.getClazz();
								final SourcedEvent o = Json.decodeValue(event.getPayload(), clazz);
								if(id.equals(o.getId())) aggregate.apply(o);
							} catch (Exception ignored) { }
						});
				return Observable.just(aggregate);
			}).doOnError(Observable::error);
		}
		catch (InstantiationException | IllegalAccessException e) { return Observable.error(e); }
	}

	@Override public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList()
	{
		return getPersistableEventList(new JsonObject());
	}

	public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList(JsonObject query)
	{
		return mongoClient.findObservable("events", query)
				.map(jsonObjects -> {
					List<PersistableEvent<? extends SourcedEvent>> events = new LinkedList<>();
					for (JsonObject event : jsonObjects) {
						try {
							events.add(makePersistableEventFromJson(event));
						} catch (ClassNotFoundException ignored) { }
					}
					return events;
				});
	}

	private PersistableEvent<? extends SourcedEvent> makePersistableEventFromJson(JsonObject event) throws ClassNotFoundException {
		//noinspection unchecked
		Class<? extends SourcedEvent> clazz
				= (Class<? extends SourcedEvent>) Class.forName(event.getString("clazz"));
		return new PersistableEvent<>(clazz, event.getJsonObject("payload").encode());
	}

	public <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event) {
		JsonObject document = new JsonObject();
		document.put("_id", event.getId());
		document.put("clazz", event.getClazz().getCanonicalName());
		document.put("payload", new JsonObject(event.getPayload()));
		return mongoClient.insertObservable("events", document).flatMap(s -> Observable.just(event));
	}
}
