package io.resx.core;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.java.Log;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Log
@Getter
@Setter
public class Aggregate
{
	private String id;

	public <T> Aggregate apply(final T event) {
		final Method[] methods = getClass().getMethods();
		for (final Method method : methods)
		{
			final Class<?>[] parameterTypes = method.getParameterTypes();

			if(!method.getName().equals("apply")
					&& parameterTypes.length == 1
					&& parameterTypes[0].equals(event.getClass()))
			{
				try
				{
					method.invoke(this, event);
				}
				catch (IllegalAccessException | InvocationTargetException e)
				{
					log.info(e.getMessage());
				}
			}
		}
		return this;
	}
}
