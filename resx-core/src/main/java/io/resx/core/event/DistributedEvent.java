package io.resx.core.event;

public class DistributedEvent
{
	private final String address;

	public DistributedEvent(String address) {
		this.address = address;
	}

	public String getAddress() {
		return address;
	}
}
