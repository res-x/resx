package io.resx.core;

import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.ext.mongo.MongoClient;
import lombok.extern.java.Log;
import rx.Observable;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

@Log
public class MongoEventStore extends AbstractEventStore
{
	private final MongoClient mongoClient;

	public MongoEventStore(Vertx vertx, EventBus eventBus) {
		super(eventBus);
		final JsonObject config = new JsonObject();
		mongoClient = MongoClient.createShared(vertx, config, "mongodb://localhost:27017");
	}

	@Override public <T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass) {
		if(aggregateCache.containsKey(id)) {
			//noinspection unchecked
			return Observable.just((T)aggregateCache.get(id));
		}

		return loadFromMongo(aggregateClass, new JsonObject().put("payload.id", id));
	}

	public <T extends Aggregate> Observable<T> load(JsonObject query, Class<T> aggregateClass) {
		String id = query.getString("payload.id");
		if(id != null && aggregateCache.containsKey(id)) {
			//noinspection unchecked
			return Observable.just((T)aggregateCache.get(id));
		}

		return loadFromMongo(aggregateClass, query);
	}

	public <T extends Aggregate, R extends SourcedEvent> Observable<T> load(JsonObject query, Class<T> aggregateClass, R event) {
		String id = query.getString("payload.id");
		if(aggregateCache.containsKey(id)) {
			Aggregate aggregate = aggregateCache.get(id);
			aggregate.apply(event);
			//noinspection unchecked
			return Observable.just((T) aggregate);
		}

		return loadFromMongo(aggregateClass, query);
	}

	private <T extends Aggregate> Observable<T> loadFromMongo(Class<T> aggregateClass, JsonObject query) {
		Observable<T> newAggregate = makeNewAggregateOf(aggregateClass);
		return newAggregate.flatMap(aggregate -> getPersistableEventList(query)
				.map(persistableEvents -> {
					persistableEvents.stream().forEach(applyEvent(aggregate));
					return aggregate;
				}));
	}

	@Override public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList()
	{
		return getPersistableEventList(new JsonObject());
	}

	public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList(JsonObject query)
	{
		return mongoClient.findObservable("events", query)
				.map(jsonObjects -> jsonObjects.stream()
						.map(this::makePersistableEventFromJson)
						.collect(Collectors.toList()));
	}

	public <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event) {
		JsonObject document = new JsonObject();
		document.put("_id", event.getId());
		document.put("dateCreated", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		document.put("clazz", event.getClazz().getCanonicalName());
		document.put("payload", new JsonObject(event.getPayload()));
		return mongoClient.insertObservable("events", document).flatMap(s -> Observable.just(event));
	}

	private PersistableEvent<? extends SourcedEvent> makePersistableEventFromJson(JsonObject event) {
		Class<? extends SourcedEvent> clazz = null;
		try {
			//noinspection unchecked
			clazz = (Class<? extends SourcedEvent>) Class.forName(event.getString("clazz"));
		} catch (ClassNotFoundException e) {
			log.warning(e.getMessage());
		}

		return new PersistableEvent<>(clazz, event.getJsonObject("payload").encode());
	}
}
