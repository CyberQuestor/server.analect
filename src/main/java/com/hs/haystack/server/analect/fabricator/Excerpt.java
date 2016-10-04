/**
 * 
 */
package com.hs.haystack.server.analect.fabricator;

import java.util.HashMap;
import java.util.Map;

import com.hs.haystack.models.common.error.runtime.analect.AnalectFabricationRuntimeException;
import com.hs.haystack.server.analect.interact.Vitae;
import com.hs.haystack.utilities.fabricator.ObjectFabricator;

/**
 * @author vinay Helps create an instance of available nlp accessors
 */
public class Excerpt {
	private static Map<Class<?>, ObjectFabricator<?>> warehouse = new HashMap<>();

	static {
		warehouse.put(Vitae.class, new ObjectFabricator<>(Vitae.class));
	}

	@SuppressWarnings("unchecked")
	public <T> T snatchSnippet(Class<T> clazz) {
		T manipulator = (T) warehouse.get(clazz).newInstance();
		if (manipulator == null) {
			throw new AnalectFabricationRuntimeException();
		}
		return manipulator;
	}

}
