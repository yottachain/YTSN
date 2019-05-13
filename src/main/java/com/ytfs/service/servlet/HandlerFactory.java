package com.ytfs.service.servlet;

import java.io.IOException;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.ClassMetadata;
import org.springframework.core.type.classreading.MetadataReader;

public class HandlerFactory {

    private static final Map<Class, Class> classMap = new HashMap();
    private static final ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(true);
    private static final Class superclass = Handler.class;

    static {
        try {
            regHandler();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(0);
        }
    }

    public static Handler getHandler(Object command) throws Throwable {
        Class cls = classMap.get(command.getClass());
        if (cls == null) {
            throw new IOException("Invalid instruction.");
        } else {
            Handler handler = (Handler) cls.getConstructor().newInstance();
            handler.setRequest(command);
            return handler;
        }
    }

    private synchronized static void regHandler() throws Exception {
        ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resourcePatternResolver.getResources("classpath*:com/ytfs/service/servlet/**/*.class");
        for (Resource r : resources) {
            if (r.isReadable()) {
                try {
                    checkResource(r);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static boolean isHandler(Class parent) {
        while ((parent = parent.getSuperclass()) != null) {
            if (parent == superclass) {
                return true;
            }
        }
        return false;
    }

    private static void checkResource(Resource resource) throws IOException, ClassNotFoundException {
        MetadataReader metadataReader = provider.getMetadataReaderFactory().getMetadataReader(resource);
        ClassMetadata metadata = metadataReader.getClassMetadata();
        if (metadata.isConcrete() && metadata.hasSuperClass()) {
            try {
                Class cls = Class.forName(metadata.getClassName());
                if (isHandler(cls)) {
                    putHandlerClass(cls);
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    private static void putHandlerClass(Class cls) throws Exception {
        Type type = ((ParameterizedType) cls.getGenericSuperclass()).getActualTypeArguments()[0];
        Class reqcls = Class.forName(type.getTypeName());
        if (classMap.containsKey(reqcls)) {
            if (classMap.get(reqcls) != cls) {
                throw new IOException("'" + cls.getName() + "' initialization error, '" + type.getTypeName() + "' is repeated.");
            }
        } else {
            classMap.put(reqcls, cls);
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {
        Set<Class> set = classMap.keySet();
        for (Class s : set) {
            System.out.print(s.getName() + "---->");
            System.out.println(classMap.get(s).getName());
        }
    }
}
