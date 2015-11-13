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
	<T extends SourcedEvent> Observable<T> publish(T message, Class<T> clazz);

	<T extends DistributedEvent, R> Observable<R> publish(T message, Class<R> clazz);

	<T extends DistributedEvent, R> Observable<R> publish(String address, T message, Class<R> clazz);

	<T extends SourcedEvent> Observable<T> publish(String address, T message, Class<T> clazz);

	<T extends FailedEvent> Observable<T> publish(String address, T message);

	<T extends Aggregate> Observable<T> publish(String address, T message);

	boolean hasSendError(Message<Object> messageAsyncResult);

	<T extends DistributedEvent> MessageConsumer<String> consumer(Class<T> event, Handler<Message<String>> handler);

	<T extends Aggregate, R extends SourcedEvent> Observable<T> load(String id, Class<T> aggregateClass, R event);

	<T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass);

	<T extends Aggregate> Consumer<PersistableEvent<? extends SourcedEvent>> applyEvent(String id, Observable<T> aggregate);

	<T extends Aggregate> Observable<T> makeNewAggregateOf(Class<T> aggregateClass);

	Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList();

	<T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event);
}
