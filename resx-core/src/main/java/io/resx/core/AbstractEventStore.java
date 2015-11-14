package io.resx.core;

import io.resx.core.event.DistributedEvent;
import io.resx.core.event.FailedEvent;
import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.rxjava.core.MultiMap;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import lombok.extern.log4j.Log4j2;
import rx.Observable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.resx.core.Constants.ERROR_HEADER;
import static io.resx.core.Constants.HEADER_TRUE;

@Log4j2
abstract public class AbstractEventStore implements EventStore {
	protected final EventBus eventBus;
	protected final Map<String, Aggregate> aggregateCache = new HashMap<>();

	public AbstractEventStore(EventBus eventBus) {
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

	@Override public <T extends DistributedEvent, R extends Aggregate> Observable<R> publish(T event, R message) {
		return publish(event.getAddress(), message, message.getId());
	}

	@Override public <T extends DistributedEvent, R extends Aggregate> Observable<R> publish(T event, R message, String cacheKey) {
		aggregateCache.put(cacheKey, message);
		eventBus.publish(event.getAddress(), Json.encode(message));
		return Observable.just(message);
	}

	@Override public <T extends Aggregate> Observable<T> publish(String address, T message) {
		return publish(address, message, message.getId());
	}

	@Override public <T extends Aggregate> Observable<T> publish(String address, T message, String cacheKey) {
		aggregateCache.put(cacheKey, message);
		eventBus.publish(address, Json.encode(message));
		return Observable.just(message);
	}

	@Override public boolean hasSendError(Message<Object> messageAsyncResult) {
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

	@Override public <T extends Aggregate, R extends SourcedEvent> Observable<T> load(String id, Class<T> aggregateClass, R event) {
		if(aggregateCache.containsKey(id)) {
			Aggregate aggregate = aggregateCache.get(id);
			aggregate.apply(event);
			//noinspection unchecked
			return Observable.just((T) aggregate);
		}

		return load(id, aggregateClass);
	}

	@Override
	public abstract <T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass);

	@Override
	public <T extends Aggregate> Consumer<PersistableEvent<? extends SourcedEvent>> applyEvent(T aggregate) {
		return event -> {
			try {
				final Class<? extends SourcedEvent> clazz = event.getClazz();
				final SourcedEvent o = Json.decodeValue(event.getPayload(), clazz);
				aggregate.apply(o);
			} catch (Exception ignored) { }
		};
	}

	@Override
	public abstract Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList();

	@Override
	public abstract <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event);

	@Override public <T extends Aggregate> Observable<T> makeNewAggregateOf(Class<T> aggregateClass) {
		try {
			return Observable.just(aggregateClass.newInstance());
		} catch (InstantiationException | IllegalAccessException e) {
			log.warn(e.getMessage());
			return Observable.error(new RuntimeException("could not create aggregate of type " + aggregateClass.getName()));
		}
	}
}
