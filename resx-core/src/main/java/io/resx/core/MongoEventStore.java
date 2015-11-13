package io.resx.core;

import io.resx.core.event.FailedEvent;
import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.ext.mongo.MongoClient;
import lombok.extern.java.Log;
import rx.Observable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Log
public class MongoEventStore extends AbstractEventStore
{
	private final MongoClient mongoClient;
	private final Map<String, Aggregate> aggregateCache = new HashMap<>();

	public MongoEventStore(Vertx vertx, EventBus eventBus) {
		super(eventBus);
		final JsonObject config = new JsonObject();
		mongoClient = MongoClient.createShared(vertx, config, "mongodb://localhost:27017");
	}

	@Override public <T extends Aggregate> Observable<T> publish(String address, T message) {
		aggregateCache.put(message.getId(), message);
		eventBus.publish(address, Json.encode(message));
		return Observable.just(message);
	}

	@Override public <T extends Aggregate, R extends SourcedEvent> Observable<T> load(String id, Class<T> aggregateClass, R event) {
		if(aggregateCache.containsKey(id)) {
			Aggregate aggregate = aggregateCache.get(id);
			aggregate.apply(event);
			//noinspection unchecked
			return Observable.just((T) aggregate);
		}

		return load(id, aggregateClass);
	}

	@Override public <T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass) {
		if(aggregateCache.containsKey(id)) {
			//noinspection unchecked
			return Observable.just((T)aggregateCache.get(id));
		}
		try
		{
			T aggregate = aggregateClass.newInstance();

			JsonObject query = new JsonObject();
			query.put("payload.id", id);

			return getPersistableEventList(query).flatMap(persistableEvents -> {
				persistableEvents.stream()
						.forEach(applyEvent(id, aggregate));
				if(aggregate.getId() != null && !"".equals(aggregate.getId()))
					aggregateCache.put(aggregate.getId(), aggregate);
				log.info("Loaded aggregate " + aggregateClass.getName());
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

	public <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event) {
		JsonObject document = new JsonObject();
		document.put("_id", event.getId());
		document.put("clazz", event.getClazz().getCanonicalName());
		document.put("payload", new JsonObject(event.getPayload()));
		return mongoClient.insertObservable("events", document).flatMap(s -> Observable.just(event));
	}

	private PersistableEvent<? extends SourcedEvent> makePersistableEventFromJson(JsonObject event) throws ClassNotFoundException {
		//noinspection unchecked
		Class<? extends SourcedEvent> clazz
				= (Class<? extends SourcedEvent>) Class.forName(event.getString("clazz"));
		return new PersistableEvent<>(clazz, event.getJsonObject("payload").encode());
	}
}
