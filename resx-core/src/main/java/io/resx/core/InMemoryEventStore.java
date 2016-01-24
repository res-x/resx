package io.resx.core;

import io.resx.core.event.PersistableEvent;
import io.resx.core.event.SourcedEvent;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.EventBus;
import rx.Observable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class InMemoryEventStore extends AbstractEventStore
{
	private final Map<String, List<PersistableEvent<? extends SourcedEvent>>> eventList = new HashMap<>();

	public InMemoryEventStore(final Vertx vertx, final String aggregatePackage, final String eventPackage) {
		super(vertx, eventPackage);
	}

	@Override
	public <T extends Aggregate> Observable<T> load(final String id, final Class<T> aggregateClass, boolean useCache) {
		final Observable<T> newAggregate = makeNewAggregateOf(aggregateClass);
		return newAggregate.flatMap(aggregate -> getPersistableEventList(id)
				.flatMap(persistableEvents -> {
					persistableEvents.stream().forEach(applyEvent(aggregate));
					return Observable.just(aggregate);
				})).doOnError(Observable::error);
	}

	@Override
	public <T extends Aggregate> Observable<List<Observable<T>>> loadAll(Class<T> aggregateClass, boolean useCache) {
		return null;
	}

	@Override public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList()
	{
		return getPersistableEventList(null);
	}

	public Observable<List<PersistableEvent<? extends SourcedEvent>>> getPersistableEventList(final String id)
	{
		final List<PersistableEvent<? extends SourcedEvent>> eventList = new LinkedList<>();
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

	public <T extends PersistableEvent<? extends SourcedEvent>> Observable<T> insert(final T event) {
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
