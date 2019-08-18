package test

import java.nio.file.Paths
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowBroadcast
import nextflow.Session
import nextflow.executor.Executor
import nextflow.executor.ExecutorFactory
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.script.BaseScript
import nextflow.script.ChannelOut
import nextflow.script.ScriptRunner
import nextflow.script.ScriptType

class MockScriptRunner extends ScriptRunner {

    MockScriptRunner() {
        super(new MockSession())
    }

    MockScriptRunner setScript(String str) {
        def script = TestHelper.createInMemTempFile('main.nf', str)
        setScript(script)
        return this
    }

    MockScriptRunner invoke() {
        execute()
        return this
    }

    BaseScript getScript() { getScriptObj() }

    @Override
    def normalizeOutput(output) {
        if( output instanceof ChannelOut ) {
            def list = new ArrayList(output.size())
            for( int i=0; i<output.size(); i++ ) {
                list.add(read0(output[i]))
            }
            return list.size() == 1 ? list[0] : list
        }

        if( output instanceof Object[] || output instanceof List) {
            def result = new ArrayList<>(output.size())
            for( def item : output ) {
                ((List)result).add(read0(item))
            }
            return result
        }

        else {
            return read0(output)
        }
    }


    private read0( obj ) {
        if( obj instanceof DataflowBroadcast )
            return obj.createReadChannel()
        return obj
    }

}

class MockSession extends Session {

    @Override
    Session start() {
        this.executorFactory = new MockExecutorFactory()
        return super.start()
    }

    MockSession() {
        super()
    }

}

class MockExecutorFactory extends ExecutorFactory {
    @Override
    protected Class<? extends Executor> getExecutorClass(String executorName) {
        return MockExecutor
    }

    @Override
    protected boolean isTypeSupported(ScriptType type, Object executor) {
        true
    }
}

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class MockExecutor extends Executor {

    @Override
    void signal() { }

    protected TaskMonitor createTaskMonitor() {
        new MockMonitor(session: session)
    }

    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        return new MockTaskHandler(task)
    }
}

@Slf4j
class MockMonitor implements TaskMonitor {

    private Session session
    private BlockingQueue<TaskHandler> queue = new LinkedBlockingQueue()

    /**
     * Start the monitoring activity for the queued tasks
     * @return The instance itself, useful to chain methods invocation
     */
    @Override
    TaskMonitor start() {
        log.debug ">>> barrier register (monitor: ${this})"
        session.barrier.register(this)
        Thread.startDaemon('MockSubmitterThread') {
            executeTasks()
        }
        return this
    }

    protected void executeTasks() {
        try {
            while( true ) {
                if( session.isTerminated() || session.isAborted() )
                    break
                TaskHandler t = queue.poll(100, TimeUnit.MILLISECONDS)
                t?.submit()
            }
        }
        finally {
            log.debug "Tasks queue = $queue"
            log.debug "<<< barrier arrives (monitor: ${this})"
            session.barrier.arrive(this)
        }
    }

    void schedule(TaskHandler handler) {
        log.debug "Task received = $handler"
        queue.add(handler)
    }

    /**
     * Remove the {@code TaskHandler} instance from the queue of tasks to be processed
     *
     * @param handler A not null {@code TaskHandler} instance
     */
    boolean evict(TaskHandler handler) {
        return true
    }

    /**
     * Notify when a task terminates
     */
    void signal() { }
}

@Slf4j
class MockTaskHandler extends TaskHandler {

    protected MockTaskHandler(TaskRun task) {
        super(task)
    }

    @Override
    void submit() {
        log.info ">> launching mock task: ${task}"
        if( task.type == ScriptType.SCRIPTLET ) {
            task.workDir = Paths.get('.').complete()
            task.stdout = task.script
            task.exitStatus = 0
        }
        else {
            task.code.call()
        }
        status = TaskStatus.COMPLETED
        task.processor.finalizeTask(task)
    }

    @Override
    boolean checkIfRunning() {
        return false
    }

    @Override
    boolean checkIfCompleted() {
        true
    }

    @Override
    void kill() { }

}
