package io.resx.core;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.java.Log;
import org.apache.commons.beanutils.MethodUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Log
@Getter
@Setter
public class Aggregate
{
	private String id;

	public <T> void apply(T event) {
		final Method[] methods = getClass().getMethods();
		for (Method method : methods)
		{
			final Method matchingAccessibleMethod = MethodUtils
					.getMatchingAccessibleMethod(this.getClass(), method.getName(), new Class<?>[] { event.getClass() });

			if(matchingAccessibleMethod != null
					&& !matchingAccessibleMethod.getParameterTypes()[0].equals(Object.class)
					&& !"apply".equals(method.getName())) {
				try
				{
					matchingAccessibleMethod.invoke(this, event);
					return;
				}
				catch (IllegalAccessException | InvocationTargetException e)
				{
					log.info(e.getMessage());
				}
			}
		}
	}
}
