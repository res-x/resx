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

	public AbstractEventStore(final EventBus eventBus) {
		this.eventBus = eventBus;
	}

	@Override
	public <T extends SourcedEvent> Observable<T> publishSourcedEvent(final T message, final Class<T> clazz) {
		return publishSourcedEvent(message.getAddress(), message, clazz);
	}

	@Override
	public <T extends DistributedEvent, R> Observable<R> publish(final T message, final Class<R> clazz) {
		return publish(message.getAddress(), message, clazz);
	}

	@Override
	public <T extends DistributedEvent, R> Observable<R> publish(final String address, final T message, final Class<R> clazz) {
		return eventBus.sendObservable(address, Json.encode(message))
				.flatMap(objectMessage -> {
					final String messageBody = (String) objectMessage.body();
					if(!hasSendError(objectMessage)) {
						//noinspection unchecked
						final R entity = clazz.equals(String.class)
								? (R) messageBody
								: Json.decodeValue(messageBody, clazz);
						return Observable.just(entity);
					}
					return Observable.error(new RuntimeException(messageBody));
				});
	}

	@Override
	public <T extends SourcedEvent> Observable<T> publishSourcedEvent(final String address, final T message, final Class<T> clazz) {
		eventBus.publish(address, Json.encode(message));
		final PersistableEvent<T> tPersistableEvent = new PersistableEvent<>(clazz, Json.encode(message));
		if(aggregateCache.containsKey(message.getId())) {
			final Aggregate aggregate = aggregateCache.get(message.getId());
			aggregate.apply(message);
		}
		return insert(tPersistableEvent).map(tPersistableEvent1 -> message);
	}

	@Override
	public <T extends FailedEvent> Observable<T> publish(final String address, final T message) {
		eventBus.publish(address, Json.encode(message));
		final PersistableEvent<? extends FailedEvent> persistableEvent = new PersistableEvent<>(message.getClass(), Json.encode(message));
		return insert(persistableEvent).map(tPersistableEvent1 -> message);
	}

	@Override
	public <T extends DistributedEvent, R extends Aggregate> Observable<R> publish(final T event, final R message) {
		return publish(event.getAddress(), message, message.getId());
	}

	@Override
	public <T extends DistributedEvent, R extends Aggregate> Observable<R> publish(final T event, final R message, final String cacheKey) {
		aggregateCache.put(cacheKey, message);
		eventBus.publish(event.getAddress(), Json.encode(message));
		return Observable.just(message);
	}

	@Override
	public <T extends Aggregate> Observable<T> publish(final String address, final T message) {
		return publish(address, message, message.getId());
	}

	@Override
	public <T extends Aggregate> Observable<T> publish(final String address, final T message, final String cacheKey) {
		aggregateCache.put(cacheKey, message);
		eventBus.publish(address, Json.encode(message));
		return Observable.just(message);
	}

	@Override
	public boolean hasSendError(final Message<Object> messageAsyncResult) {
		final MultiMap headers = messageAsyncResult.headers();
		return HEADER_TRUE.equals(headers.get(ERROR_HEADER));
	}

	@Override
	public <T extends DistributedEvent> MessageConsumer<String> consumer(final Class<T> event, final Handler<Message<String>> handler) {
		try
		{
			return eventBus.consumer(event.newInstance().getAddress(), handler);
		}
		catch (InstantiationException | IllegalAccessException ignored) { }
		return null;
	}

	@Override
	public <T extends Aggregate, R extends SourcedEvent> Observable<T> load(final String id, final Class<T> aggregateClass, final R event) {
		if(aggregateCache.containsKey(id)) {
			final Aggregate aggregate = aggregateCache.get(id);
			aggregate.apply(event);
			//noinspection unchecked
			return Observable.just((T) aggregate);
		}

		return load(id, aggregateClass);
	}

	@Override
	public abstract <T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass);

	@Override
	public <T extends Aggregate> Observable<List<Observable<T>>> loadAll(Class<T> aggregateClass) {
		return loadAll(aggregateClass, true);
	}

	@Override
	public <T extends Aggregate> Consumer<PersistableEvent<? extends SourcedEvent>> applyEvent(final T aggregate) {
		return event -> {
			try {
				final Class<? extends SourcedEvent> clazz = event.getClazz();
				final SourcedEvent o = Json.decodeValue(event.getPayload(), clazz);
				aggregate.apply(o);
			} catch (final Exception ignored) {
			}
		};
	}

	@Override
	public abstract Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList();

	@Override
	public abstract <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event);

	@Override
	public <T extends Aggregate> Observable<T> makeNewAggregateOf(final Class<T> aggregateClass) {
		try {
			return Observable.just(aggregateClass.newInstance());
		} catch (InstantiationException | IllegalAccessException e) {
			log.warn(e.getMessage());
			return Observable.error(new RuntimeException("could not create aggregate of type " + aggregateClass.getName()));
		}
	}

	@Override
	public void cacheAggregate(Aggregate aggregate) {
		aggregateCache.put(aggregate.getId(), aggregate);
	}
}
