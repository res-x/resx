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
import io.vertx.rxjava.ext.mongo.MongoClient;
import rx.Observable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static io.resx.core.Constants.ERROR_HEADER;
import static io.resx.core.Constants.HEADER_TRUE;

public class InMemoryEventStore implements EventStore
{
	private final EventBus eventBus;
	private final TreeMap<String, List<PersistableEvent<? extends SourcedEvent>>> treeMap = new TreeMap<>();

	public InMemoryEventStore(EventBus eventBus) {
		this.eventBus = eventBus;
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

	@Override public <T extends DistributedEvent> void consumer(Class<T> event, Handler<Message<String>> handler) {
		try
		{
			eventBus.consumer(event.newInstance().getAddress(), handler);
		}
		catch (InstantiationException | IllegalAccessException ignored) { }
	}

	@Override public <T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass) {
		try
		{
			T aggregate;
			aggregate = aggregateClass.newInstance();

			return getPersistableEventList(id).flatMap(persistableEvents -> {
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
		return getPersistableEventList(null);
	}

	public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList(String id)
	{
		List<PersistableEvent<? extends SourcedEvent>> eventList = new ArrayList<>();
		if(id == null) {
			treeMap.values().forEach(eventList::addAll);
		}
		else {
			List<PersistableEvent<? extends SourcedEvent>> persistableEvents = treeMap.get(id);
			if(persistableEvents == null) persistableEvents = new ArrayList<>();
			eventList.addAll(persistableEvents);
		}

		return Observable.just(eventList);
	}

	public <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event) {
		final JsonObject jsonObject = new JsonObject(event.getPayload());
		final String id = jsonObject.getString("id");
		if(treeMap.containsKey(id)) {
			treeMap.get(id).add(event);
		}
		else {
			final List<PersistableEvent<? extends SourcedEvent>> events = new ArrayList<>();
			events.add(event);
			treeMap.put(id, events);
		}
		return Observable.just(event);
	}
}
