package com.messaging.broker.systemtest.support

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

class ManagedJavaProcess {

    final String name
    final Process process
    final Path logFile
    final Path workDir

    ManagedJavaProcess(String name, Process process, Path logFile, Path workDir) {
        this.name = name
        this.process = process
        this.logFile = logFile
        this.workDir = workDir
    }

    static ManagedJavaProcess start(String name,
                                    String mainClass,
                                    Path workDir,
                                    Path logFile,
                                    Map<String, String> env = [:],
                                    String classpath = System.getProperty('java.class.path'),
                                    List<String> jvmArgs = [],
                                    List<String> args = []) {
        Files.createDirectories(workDir)
        Files.createDirectories(logFile.parent)

        String javaBin = Path.of(System.getProperty('java.home'), 'bin', 'java').toString()
        List<String> command = ([javaBin] + jvmArgs + ['-cp', classpath, mainClass] + args)
            .collect { it.toString() }

        ProcessBuilder pb = new ProcessBuilder(command)
        pb.directory(workDir.toFile())
        pb.redirectErrorStream(true)
        pb.redirectOutput(logFile.toFile())
        env.each { k, v ->
            pb.environment().put(k.toString(), v?.toString() ?: '')
        }

        new ManagedJavaProcess(name, pb.start(), logFile, workDir)
    }

    boolean isAlive() {
        process?.isAlive()
    }

    String readLog() {
        Files.exists(logFile) ? Files.readString(logFile) : ''
    }

    int exitCode() {
        process.exitValue()
    }

    void stop() {
        if (process == null) {
            return
        }
        if (process.isAlive()) {
            process.destroy()
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly()
                process.waitFor(5, TimeUnit.SECONDS)
            }
        }
    }
}
