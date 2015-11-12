package io.resx.core;

import io.resx.core.event.DistributedEvent;
import io.resx.core.event.FailedEvent;
import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.MultiMap;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import rx.Observable;

import java.util.*;

import static io.resx.core.Constants.ERROR_HEADER;
import static io.resx.core.Constants.HEADER_TRUE;

public class InMemoryEventStore extends AbstractEventStore
{
	private final Map<String, List<PersistableEvent<? extends SourcedEvent>>> eventList = new HashMap<>();

	public InMemoryEventStore(EventBus eventBus) {
		super(eventBus);
	}

	@Override public <T extends Aggregate> Observable<T> load(String id, Class<T> aggregateClass) {
		try
		{
			T aggregate;
			aggregate = aggregateClass.newInstance();

			return getPersistableEventList(id).flatMap(persistableEvents -> {
				persistableEvents.stream()
						.filter(event -> !(FailedEvent.class.isAssignableFrom(event.getClazz())))
						.forEach(applyEvent(id, aggregate));
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
		List<PersistableEvent<? extends SourcedEvent>> eventList = new LinkedList<>();
		if(id == null) {
			this.eventList.values().forEach(eventList::addAll);
		}
		else {
			List<PersistableEvent<? extends SourcedEvent>> persistableEvents = this.eventList.get(id);
			if(persistableEvents == null) persistableEvents = new LinkedList<>();
			eventList.addAll(persistableEvents);
		}

		return Observable.just(eventList);
	}

	public <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(T event) {
		final JsonObject jsonObject = new JsonObject(event.getPayload());
		final String id = jsonObject.getString("id");
		if(eventList.containsKey(id)) {
			eventList.get(id).add(event);
		}
		else {
			final List<PersistableEvent<? extends SourcedEvent>> events = new LinkedList<>();
			events.add(event);
			eventList.put(id, events);
		}
		return Observable.just(event);
	}
}
