package io.resx.core.command;

import io.resx.core.event.DistributedEvent;

public class Command extends DistributedEvent
{
	public Command(final String address)
	{
		super(address);
	}
}
