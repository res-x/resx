package io.resx.core;

import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.ext.mongo.MongoClient;
import lombok.extern.log4j.Log4j2;
import rx.Observable;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Log4j2
public class MongoEventStore extends AbstractEventStore
{
	private final MongoClient mongoClient;

	public MongoEventStore(final Vertx vertx, final EventBus eventBus, final JsonObject config) {
		super(eventBus);
		mongoClient = MongoClient.createShared(vertx, config);
	}

	@Override
	public <T extends Aggregate> Observable<T> load(final String id, final Class<T> aggregateClass) {
		if(aggregateCache.containsKey(id)) {
			//noinspection unchecked
			return Observable.just((T)aggregateCache.get(id));
		}

		return loadFromMongo(aggregateClass, new JsonObject().put("payload.id", id));
	}

	public <T extends Aggregate> Observable<T> load(final JsonObject query, final Class<T> aggregateClass) {
		return loadFromMongo(aggregateClass, query);
	}

	public <T extends Aggregate, R extends SourcedEvent> Observable<T> load(final JsonObject query, final Class<T> aggregateClass, final R event) {
		final String id = query.encode();
		if(aggregateCache.containsKey(id)) {
			final Aggregate aggregate = aggregateCache.get(id);
			aggregate.apply(event);
			//noinspection unchecked
			return Observable.just((T) aggregate);
		}

		return loadFromMongo(aggregateClass, query);
	}

	private <T extends Aggregate> Observable<T> loadFromMongo(final Class<T> aggregateClass, final JsonObject query) {
		final Observable<T> newAggregate = makeNewAggregateOf(aggregateClass);
		return newAggregate.flatMap(aggregate -> getPersistableEventList(query)
				.flatMap(persistableEvents -> {
					persistableEvents.stream().forEach(applyEvent(aggregate));
					return Observable.just(aggregate);
				}));
	}

	@Override public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList()
	{
		return getPersistableEventList(new JsonObject());
	}

	public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList(final JsonObject query)
	{
		return mongoClient.findObservable("events", query)
				.map(jsonObjects -> jsonObjects.stream()
						.map(this::makePersistableEventFromJson)
						.collect(Collectors.toList()));
	}

	public <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(final T event) {
		final JsonObject document = new JsonObject();
		document.put("_id", event.getId());
		document.put("dateCreated", new Date().getTime());
		document.put("clazz", event.getClazz().getCanonicalName());
		document.put("payload", new JsonObject(event.getPayload()));
		return mongoClient.insertObservable("events", document).flatMap(s -> Observable.just(event));
	}

	private PersistableEvent<? extends SourcedEvent> makePersistableEventFromJson(final JsonObject event) {
		Class<? extends SourcedEvent> clazz = null;
		try {
			//noinspection unchecked
			clazz = (Class<? extends SourcedEvent>) Class.forName(event.getString("clazz"));
		} catch (final ClassNotFoundException e) {
			log.warn(e.getMessage());
		}

		return new PersistableEvent<>(clazz, event.getJsonObject("payload").encode());
	}
}
