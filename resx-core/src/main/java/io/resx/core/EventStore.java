package io.resx.core;

import io.resx.core.event.DistributedEvent;
import io.resx.core.event.FailedEvent;
import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import rx.Observable;

import java.util.List;
import java.util.function.Consumer;

public interface EventStore
{
	<T extends SourcedEvent> Observable<T> publishSourcedEvent(T message, Class<T> clazz);

	<T extends DistributedEvent, R> Observable<R> publish(T message, Class<R> clazz);

	<T extends DistributedEvent, R> Observable<R> publish(String address, T message, Class<R> clazz);

	<T extends SourcedEvent> Observable<T> publishSourcedEvent(String address, T message, Class<T> clazz);

	<T extends FailedEvent> Observable<T> publish(String address, T message);

	<T extends DistributedEvent, R extends Aggregate> Observable<R> publish(T event, R message);

	<T extends DistributedEvent, R extends Aggregate> Observable<R> publish(T event, R message, String cacheKey);

	<T extends Aggregate> Observable<T> publish(String address, T message);

	<T extends Aggregate> Observable<T> publish(String address, T message, String cacheKey);

	boolean hasSendError(Message<Object> messageAsyncResult);

	<T extends DistributedEvent> MessageConsumer<String> consumer(Class<T> event, Handler<Message<String>> handler);

	<T extends Aggregate, R extends SourcedEvent> Observable<T> load(String id, Class<T> aggregateClass, R event);

	<T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass);

	<T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass, boolean useCache);

	<T extends Aggregate> Observable<List<Observable<T>>> loadAll(Class<T> aggregateClass);

	<T extends Aggregate> Observable<List<Observable<T>>> loadAll(Class<T> aggregateClass, boolean useCache);

	<T extends Aggregate> Consumer<PersistableEvent<? extends SourcedEvent>> applyEvent(T aggregate);

	<T extends Aggregate> Observable<T> makeNewAggregateOf(Class<T> aggregateClass);

	Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList();

	<T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event);

	void cacheAggregate(Aggregate aggregate);

	void cacheAllAggregates(String aggregatePackage);
}
