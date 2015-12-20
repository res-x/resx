package io.resx.core.event;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
public class SourcedEvent extends DistributedEvent
{
	private final Date dateCreated;
	private String id;

	public SourcedEvent(final String address, final String id)
	{
		super(address);
		this.id = id;
		dateCreated = new Date();
	}
}
