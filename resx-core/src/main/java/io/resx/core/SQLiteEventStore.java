package io.resx.core;

import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLConnection;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.util.Strings;
import rx.Observable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Log4j2
public class SQLiteEventStore extends AbstractEventStore {
	private final JDBCClient client;

	public SQLiteEventStore(final Vertx vertx, final EventBus eventBus, JsonObject config) {
		super(eventBus);
		config = (config == null)
				? new JsonObject()
				.put("url", "jdbc:sqlite:sample.db")
				.put("driver_class", "org.sqlite.JDBC")
				: config;

		client = JDBCClient.createShared(vertx, config);
		client.getConnectionObservable()
				.flatMap(sqlConnection -> getSqlConnectionObservable(sqlConnection)
						.doOnNext(sqlConnection1 -> sqlConnection1.executeObservable("CREATE TABLE IF NOT EXISTS EVENTS(\n" +
								"   ID TEXT PRIMARY KEY     NOT NULL,\n" +
								"   AGGREGATE_ID TEXT       NOT NULL,\n" +
								"   PAYLOAD        TEXT    NOT NULL,\n" +
								"   CLAZZ          TEXT    NOT NULL\n" +
								");").subscribe())
				).subscribe();
	}

	private Observable<SQLConnection> getSqlConnectionObservable(final SQLConnection sqlConnection) {
		return Observable.just(sqlConnection)
				.doOnNext(sqlConnection2 -> sqlConnection2.setAutoCommitObservable(true).subscribe())
				.doOnUnsubscribe(sqlConnection::close);
	}

	@Override
	public <T extends Aggregate> Observable<T> load(final String id, final Class<T> aggregateClass) {
		if (aggregateCache.containsKey(id)) {
			//noinspection unchecked
			return Observable.just((T) aggregateCache.get(id));
		}

		final Observable<T> newAggregate = makeNewAggregateOf(aggregateClass);

		final String query = "select * from events where aggregate_id = ?";
		final JsonArray params = new JsonArray(Collections.singletonList(id));

		return loadAggregateWithQuery(newAggregate, query, params);
	}

	private <T extends Aggregate> Observable<T> loadAggregateWithQuery(Observable<T> newAggregate, String query, JsonArray params) {
		return newAggregate.flatMap(aggregate -> client.getConnectionObservable()
				.flatMap(sqlConnection -> getSqlConnectionObservable(sqlConnection)
						.flatMap(sqlConnection1 -> sqlConnection1
								.queryWithParamsObservable(query,
										params)
								.flatMap(resultSet -> {
									resultSet.getRows()
											.stream()
											.map(this::makePersistableEventFromJson)
											.forEach(applyEvent(aggregate));
									return Strings.isNotEmpty(aggregate.getId())
											? Observable.just(aggregate)
											: Observable.just(null);
								})
								.onErrorReturn(throwable -> aggregate))));
	}

	@Override
	public <T extends Aggregate> Observable<List<Observable<T>>> loadAll(Class<T> aggregateClass) {
		return client.getConnectionObservable()
				.flatMap(sqlConnection -> getSqlConnectionObservable(sqlConnection)
						.flatMap(sqlConnection1 -> sqlConnection1
								.queryObservable("select distinct aggregate_id from events")
								.map(resultSet -> resultSet.getRows()
										.stream()
										.map(entries -> {
											final String id = entries.getString("AGGREGATE_ID");
											return load(id, aggregateClass)
													.flatMap(t -> t == null ? Observable.empty() : Observable.just(t));
										})
										.collect(Collectors.toList()))
								.onErrorReturn(throwable -> null)));
	}

	private PersistableEvent<? extends SourcedEvent> makePersistableEventFromJson(final JsonObject event) {
		Class<? extends SourcedEvent> clazz = null;
		try {
			//noinspection unchecked
			clazz = (Class<? extends SourcedEvent>) Class.forName(event.getString("CLAZZ"));
		} catch (final ClassNotFoundException e) {
			log.warn(e.getMessage());
		}

		return new PersistableEvent<>(clazz, event.getString("PAYLOAD"));
	}

	@Override
	public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList() {
		return client.getConnectionObservable()
				.flatMap(sqlConnection -> getSqlConnectionObservable(sqlConnection)
						.flatMap(sqlConnection1 -> sqlConnection1.queryObservable("select * from events")
								.map(ResultSet::getRows)
								.map(resultSet -> resultSet
										.stream()
										.map(this::makePersistableEventFromJson)
										.collect(Collectors.toList()))));
	}

	public <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(final T event) {
		final JsonObject payload = new JsonObject(event.getPayload());
		final String aggregateId = payload.getString("id");
		return client.getConnectionObservable()
				.flatMap(sqlConnection -> getSqlConnectionObservable(sqlConnection)
						.flatMap(sqlConnection1 -> sqlConnection1.updateWithParamsObservable("insert into events (id, aggregate_id, clazz, payload) values (?, ?, ?, ?)",
								new JsonArray(Arrays.asList(event.getId(), aggregateId, event.getClazz().getCanonicalName(), event.getPayload()))))
						.flatMap(s -> Observable.just(event))
						.onErrorReturn(throwable -> event));
	}
}
