// package com.pingan.pbear.util;
//
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.io.File;
// import java.io.FileInputStream;
// import java.io.IOException;
// import java.net.URL;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
// import java.util.jar.JarEntry;
// import java.util.jar.JarInputStream;
// import com.pingan.pbear.util.StringUtil;
//
//
//
// /**
//  * This scanner is used to find out all classes in a package.
//  * Used in finding UDF function auto
//  * Created by lovelife on 17/2/11.
//  */
// public class PackageScanner {
//     private Logger logger = LoggerFactory.getLogger(PackageScanner.class);
//     private String basePackage;
//     private ClassLoader classLoader;
//
//     /**
//      * Construct an instance and specify the base package it should scan.
//      * @param basePackage The base package to scan.
//      */
//     public PackageScanner(String basePackage) {
//         this.basePackage = basePackage;
//         this.classLoader = getClass().getClassLoader();
//     }
//
//     /**
//      * Get all fully qualified names located in the specified package
//      * and its sub-package.
//      *
//      * @return A list of fully qualified names.
//      * @throws IOException IO异常
//      */
//
//     public List<String> getFullyQualifiedClassNameList() throws IOException {
//         logger.info("开始扫描包{}下的所有类", basePackage);
//         String splashPath = StringUtil.dotToSplash(basePackage);
//
//         // get file path
//         URL url = classLoader.getResource(splashPath);
//         String filePath = StringUtil.getRootPath(url);
//
//         List<String> names;
//         if (isJarFile(filePath)) {
//             // jar file
//             logger.info("{} 是一个JAR包", filePath);
//             names = readFromJarFile(filePath, splashPath);
//         } else {
//             // directory
//             logger.info("{} 是一个目录", filePath);
//             names = getClassNamesFromDirectory(filePath, basePackage, new ArrayList<String>());
//         }
//         return names;
//     }
//
//     private List<String> getClassNamesFromDirectory(String filePath, String basePackage, List<String> nameList) {
//         List<String> names = readFromDirectory(filePath);
//         for (String name : names) {
//             if (isClassFile(name)) {
//                 // scala类有很多重复，包含$
//                 if (!name.contains("$")) {
//                     nameList.add(toFullyQualifiedName(name, basePackage));
//                 }
//             } else {
//                 // this is a directory
//                 // check this directory for more classes
//                 // do recursive invocation
//                 getClassNamesFromDirectory(filePath+"/"+name, basePackage+"."+name, nameList);
//             }
//         }
//         return nameList;
//     }
//
//
//     /**
//      * Convert short class name to fully qualified name.
//      * e.g., String -> java.lang.String
//      */
//     private String toFullyQualifiedName(String shortName, String basePackage) {
//
//         String trimedExtemsionName = StringUtil.trimExtension(shortName);
//         return basePackage + '.' + StringUtil.splashToDot(trimedExtemsionName);
//     }
//
//     private List<String> readFromJarFile(String jarPath, String splashedPackageName) throws IOException {
//
//         logger.info("从JAR包中读取类: {}", jarPath);
//         JarInputStream jarIn = new JarInputStream(new FileInputStream(jarPath));
//         JarEntry entry = jarIn.getNextJarEntry();
//
//         List<String> nameList = new ArrayList<String>();
//         while (null != entry) {
//             String name = entry.getName();
//             if (name.startsWith(splashedPackageName) && isClassFile(name)) {
//                 String trimedExtensionFullClassName = StringUtil.splashToDot(StringUtil.trimExtension(name));
//                 nameList.add(trimedExtensionFullClassName);
//             }
//             entry = jarIn.getNextJarEntry();
//         }
//         return nameList;
//     }
//
//     private List<String> readFromDirectory(String path) {
//         File file = new File(path);
//         String[] classNames = file.list();
//         if (null == classNames) {
//             return new ArrayList<String>();
//         }
//         return Arrays.asList(classNames);
//     }
//
//     private boolean isClassFile(String name) {
//         return name.endsWith(".class");
//     }
//
//     private boolean isJarFile(String name) {
//         return name.endsWith(".jar");
//     }
//
//     /**
//      * For test purpose.
//      */
//     public static void main(String[] args) throws Exception {
//         PackageScanner scan = new PackageScanner("com.pingan.pbear.common");
//         List<String> nameList = scan.getFullyQualifiedClassNameList();
//         for (String n : nameList) {
//             System.out.println("找到" + n);
//         }
//     }
//
// }