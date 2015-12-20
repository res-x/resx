package io.resx.core;

import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.mongo.MongoClient;
import io.vertx.rxjava.ext.sql.SQLConnection;
import lombok.extern.log4j.Log4j2;
import rx.Observable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Log4j2
public class SQLiteEventStore extends AbstractEventStore {
	private final JDBCClient client;

	public SQLiteEventStore(Vertx vertx, EventBus eventBus, JsonObject config) {
		super(eventBus);
		config = (config == null)
				? new JsonObject()
				.put("url", "jdbc:sqlite:sample.db")
				.put("driver_class", "org.sqlite.JDBC")
				: config;

		client = JDBCClient.createShared(vertx, config);
		client.getConnectionObservable()
				.flatMap(sqlConnection -> Observable.just(sqlConnection)
						.doOnNext(sqlConnection2 -> sqlConnection2.setAutoCommitObservable(true).subscribe())
						.doOnNext(sqlConnection1 -> sqlConnection1.executeObservable("CREATE TABLE IF NOT EXISTS EVENTS(\n" +
								"   ID INT PRIMARY KEY     NOT NULL,\n" +
								"   AGGREGATE_ID INT       NOT NULL,\n" +
								"   PAYLOAD        TEXT    NOT NULL,\n" +
								"   CLAZZ          TEXT    NOT NULL\n" +
								");").subscribe())
						).subscribe();
	}

	@Override
	public <T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass) {
		if (aggregateCache.containsKey(id)) {
			//noinspection unchecked
			return Observable.just((T) aggregateCache.get(id));
		}

		Observable<T> newAggregate = makeNewAggregateOf(aggregateClass);

		return newAggregate.flatMap(aggregate -> client.getConnectionObservable()
				.flatMap(sqlConnection -> sqlConnection
						.queryWithParamsObservable("select * from events where aggregate_id = :id",
								new JsonArray(Collections.singletonList(id)))
						.flatMap(resultSet -> {
							resultSet.getRows().forEach(columns -> {
								final PersistableEvent<? extends SourcedEvent> persistableEvent = makePersistableEventFromJson(columns);
								aggregate.apply(persistableEvent);
							});
							return Observable.just(aggregate);
						})
						.onErrorReturn(throwable -> aggregate)
				));
	}

	private PersistableEvent<? extends SourcedEvent> makePersistableEventFromJson(JsonObject event) {
		Class<? extends SourcedEvent> clazz = null;
		try {
			//noinspection unchecked
			clazz = (Class<? extends SourcedEvent>) Class.forName(event.getString("clazz"));
		} catch (ClassNotFoundException e) {
			log.warn(e.getMessage());
		}

		return new PersistableEvent<>(clazz, event.getJsonObject("payload").encode());
	}

	@Override
	public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList() {
		return client.getConnectionObservable()
				.flatMap(sqlConnection -> sqlConnection
						.queryObservable("select * from events")
						.map(ResultSet::getRows)
						.map(resultSet -> resultSet
								.stream()
								.map(this::makePersistableEventFromJson)
								.collect(Collectors.toList())));

	}

	public <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event) {
		final JsonObject payload = new JsonObject(event.getPayload());
		final String aggregateId = payload.getString("id");
		return client.getConnectionObservable()
				.flatMap(sqlConnection -> sqlConnection
						.updateWithParamsObservable("insert into events (id, aggregate_id, clazz, payload) values (?, ?, ?, ?)",
								new JsonArray(Arrays.asList(event.getId(), aggregateId, event.getClazz().getCanonicalName(), event.getPayload()))))
				.flatMap(s -> Observable.just(event))
				.onErrorReturn(throwable -> event);
	}
}
