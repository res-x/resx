package io.resx.core.event;

import lombok.Getter;
import lombok.Setter;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

@Getter
@Setter
public class SourcedEvent extends DistributedEvent
{
	private String id;
	private final Date _dateCreated;

	public SourcedEvent(String address, String id)
	{
		super(address);
		this.id = id;
		_dateCreated = new Date();
	}
}
