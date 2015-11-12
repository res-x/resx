package io.resx.core.event;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class FailedEvent extends SourcedEvent {
	private final String message;
	public FailedEvent(String address, String message) {
		super(address, null);
		this.message = message;
	}
}
