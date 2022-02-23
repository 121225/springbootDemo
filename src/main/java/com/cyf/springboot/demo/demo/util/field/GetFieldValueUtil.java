package com.cyf.springboot.demo.demo.util.field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * created by CAIYANFENG on 23/02/2022
 * 获取属性名称获取属性值
 */
public class GetFieldValueUtil {
    private static final Logger logger = LoggerFactory.getLogger(GetFieldValueUtil.class);

    /**
     * 获取属性
     * @param cls
     * @param fieldName
     * @return
     */
    public static Field getField(Class<?> cls, String fieldName) {
        try {
            Field field = cls.getDeclaredField(fieldName);
            field.setAccessible(true);

            return field;
        } catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException("cannot find or access field '" + fieldName + "' from " + cls.getName(), e);
        }
    }

    /**
     * 获取值
     * @param obj
     * @param field
     * @param <T>
     * @return
     */
    @SuppressWarnings({"unchecked" })
    public static <T> T getValue(Object obj, Field field) {
        try {
            return (T)field.get(obj);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            logger.error("get value fail", e);
            throw new RuntimeException(e);
        }
    }

}
