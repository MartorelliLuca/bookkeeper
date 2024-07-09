package integration;

import org.apache.bookkeeper.bookie.datainteg.DataIntegrityCheck;
import org.apache.bookkeeper.bookie.datainteg.DataIntegrityService;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IntegrationTest {
    LifecycleComponentStack lifecycleComponentStack;
    LifecycleListenerTestImpl listener;

    private LifecycleComponentStack getLifecycleComponentStack(List<LifecycleComponent> components) {
        LifecycleComponentStack.Builder builder = LifecycleComponentStack.newBuilder();

        String name = "myStack";

        builder.withName(name);

        for(LifecycleComponent component: components) {
            builder.addComponent(component);
        }

        return builder.build();
    }

    private DataIntegrityService getLimitedDataIntegrityService(BookieConfiguration conf, StatsLogger statsLogger, DataIntegrityCheck check) {
        return new DataIntegrityService(conf, statsLogger, check) {
            @Override
            public int interval() {
                return 500; // mezzo secondo
            }
            @Override
            public TimeUnit intervalUnit() {
                return TimeUnit.MILLISECONDS;
            }
        };
    }

    private void validSetUp() throws Exception {
        StatsLogger statsLogger = NullStatsLogger.INSTANCE; // È un logger di statistiche fittizio originariamente presente utilizzato per i test

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());

        DataIntegrityCheck check = mock(DataIntegrityCheck.class);  // L'implementazione richiede di istanziare un bookie
        LifecycleComponent component1 = getLimitedDataIntegrityService(conf, statsLogger, check);

        LifecycleComponent component2 = new StatsProviderService(conf);

        List<LifecycleComponent> components = Arrays.asList(component1, component2);

        lifecycleComponentStack = getLifecycleComponentStack(components);

        listener = new LifecycleListenerTestImpl();
        lifecycleComponentStack.addLifecycleListener(listener);
    }

    private void invalidSetUp(Class<? extends Throwable> t) throws Exception {
        StatsLogger statsLogger = NullStatsLogger.INSTANCE; // È un logger di statistiche fittizio originariamente presente utilizzato per i test

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());

        DataIntegrityCheck check = mock(DataIntegrityCheck.class);
        CompletableFuture<Void> future = mock(CompletableFuture.class);
        when(check.needsFullCheck()).thenReturn(true);
        when(check.runFullCheck()).thenReturn(future);
        when(future.get()).thenThrow(t);

        LifecycleComponent component1 = getLimitedDataIntegrityService(conf, statsLogger, check);

        LifecycleComponent component2 = new StatsProviderService(conf);

        List<LifecycleComponent> components = Arrays.asList(component1, component2);

        lifecycleComponentStack = getLifecycleComponentStack(components);

        listener = new LifecycleListenerTestImpl();
        lifecycleComponentStack.addLifecycleListener(listener);
    }

    @Test
    public void testStartAndStop() throws Exception {

        validSetUp();

        lifecycleComponentStack.start();    // Se i componenti non sono nello stato Started, non possono essere stoppati.

        lifecycleComponentStack.stop();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STOPPED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

    }

    // Questo test copre il caso in cui il DataIntegrityService ha un problema. Il componente non dovrebbe essere interrotto.
    @Test
    public void testInvalidStartAndStop() throws Exception {

        invalidSetUp(InterruptedException.class);

        lifecycleComponentStack.start();    // Se i componenti non sono nello stato STARTED, non possono essere stoppati.

        lifecycleComponentStack.stop();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STOPPED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }
    }


    // Questo test copre il caso in cui il DataIntegrityService ha un problema. Il componente non dovrebbe essere interrotto.
    @Test
    public void testInvalid2StartAndStop() throws Exception {

        invalidSetUp(ExecutionException.class);

        lifecycleComponentStack.start();    // Se i componenti non sono nello stato STARTED, non possono essere stoppati.

        lifecycleComponentStack.stop();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STOPPED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

    }


    @Test
    public void testClose() throws Exception {

        validSetUp();

        lifecycleComponentStack.close();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

    }

    @Test
    public void testStart() throws Exception {

        validSetUp();

        lifecycleComponentStack.start();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STARTED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

    }

    // Questo test simula un'eccezione in un componente e come reagisce lo stack. Viene configurato anche un ExceptionHandler, quindi il test verifica
    // anche che il gestore sia configurato correttamente nei componenti.
    @Test
    public void testFail1() throws Exception {

        failSetUp();

        Semaphore semaphore = new Semaphore(1); // Una semaforo viene utilizzato per vedere se il gestore è stato chiamato

        lifecycleComponentStack.setExceptionHandler((t, e) -> semaphore.release());

        semaphore.acquire();

        lifecycleComponentStack.start();

        semaphore.acquire();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.close();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

        // Verificando se tutte le operazioni del listener (prima e dopo ciascuna operazione in ciascun componente) sono state eseguite.
        Assert.assertEquals("This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeClose\n" +
                "This is the method afterClose\n" +
                "This is the method beforeClose\n" +
                "This is the method afterClose\n" +
                "This is the method beforeClose\n" +
                "This is the method afterClose\n", listener.getLog());
    }

    // Questo test simula un'eccezione in un componente e come reagisce lo stack. Nessun ExceptionHandler è configurato, quindi il metodo start() deve
    // lanciare un'eccezione.
    @Test
    public void testFail2() throws Exception {

        failSetUp();
        try {
            lifecycleComponentStack.start();
        } catch (RuntimeException e) {
            // catturando l'eccezione
        }

        // Verificando se tutte le operazioni del listener (prima e dopo ciascuna operazione in ciascun componente) sono state eseguite.
        Assert.assertEquals("This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStart\n", listener.getLog());
    }

    private void failSetUp() throws Exception {
        StatsLogger statsLogger = NullStatsLogger.INSTANCE; // È un logger di statistiche fittizio originariamente presente utilizzato per i test

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());
        DataIntegrityCheck check = mock(DataIntegrityCheck.class);  // L'implementazione richiede di istanziare un bookie
        LifecycleComponent component1 = getLimitedDataIntegrityService(conf, statsLogger, check);
        LifecycleComponent component2 = new StatsProviderService(conf);

        LifecycleComponent component3 = new DataIntegrityService(conf, statsLogger, check) {
            @Override
            public int interval() {
                return 500; // mezzo secondo
            }
            @Override
            public TimeUnit intervalUnit() {
                return TimeUnit.MILLISECONDS;
            }
            @Override
            public void doStart() {
                throw new RuntimeException("start failure");
            }
        };

        List<LifecycleComponent> components = Arrays.asList(component1, component2, component3);

        lifecycleComponentStack = getLifecycleComponentStack(components);

        listener = new LifecycleListenerTestImpl();
        lifecycleComponentStack.addLifecycleListener(listener);
    }

    private void failSetUp2() throws Exception {
        StatsLogger statsLogger = NullStatsLogger.INSTANCE; //Dummy stats logger

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());
        DataIntegrityCheck check = mock(DataIntegrityCheck.class);  //The implementation require to instantiate a bookie
        LifecycleComponent component1 = getLimitedDataIntegrityService(conf, statsLogger, check);
        LifecycleComponent component2 = new StatsProviderService(conf);

        LifecycleComponent component3 = new DataIntegrityService(conf, statsLogger, check) {
            @Override
            public int interval() {
                return 500; //half second
            }
            @Override
            public TimeUnit intervalUnit() {
                return TimeUnit.MILLISECONDS;
            }
            @Override
            public void doStop() {
                close();
            }
        };

        List<LifecycleComponent> components = Arrays.asList(component1, component2, component3);

        lifecycleComponentStack = getLifecycleComponentStack(components);

        listener = new LifecycleListenerTestImpl();
        lifecycleComponentStack.addLifecycleListener(listener);
    }

    @Test
    public void testLifecycle1() throws Exception {
        //This test tests the normal case when the lifecycle is respected

        validSetUp();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.close();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

        //Verifying if all the listener operation (before and after each operation in each component) have been executed.
        Assert.assertEquals("This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeClose\n" +
                "This is the method afterClose\n" +
                "This is the method beforeClose\n" +
                "This is the method afterClose\n", listener.getLog());
    }

    @Test
    public void testLifecycle2() throws Exception {
        //This test try to restart the components

        validSetUp();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.close();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

        //Verifying if all the listener operation (before and after each operation in each component) have been executed.
        Assert.assertEquals("This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeClose\n" +
                "This is the method afterClose\n" +
                "This is the method beforeClose\n" +
                "This is the method afterClose\n", listener.getLog());

    }

    @Test
    public void testLifecycle3() throws Exception {
        //This test try to restart closed components

        validSetUp();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.close();

        //Closed components cannot be restarted, so an exception is expected.
        boolean condition = false;
        try {
            lifecycleComponentStack.start();
        } catch (IllegalStateException e) {
            condition = true;
        }
        Assert.assertTrue(condition);

    }

    @Test
    public void testLifecycle4() throws Exception {
        //This test try to close started components

        validSetUp();

        lifecycleComponentStack.start();

        //Started components cannot be closed (must be stopped before), but this behaviour is managed.
        // So, before to goes in the CLOSED state, each component should go in the STOPPED state.

        lifecycleComponentStack.close();

        //Verifying if all the listener operation (before and after each operation in each component) have been executed.
        Assert.assertEquals("This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStart\n" +
                "This is the method afterStart\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeClose\n" +
                "This is the method afterClose\n" +
                "This is the method beforeStop\n" +
                "This is the method afterStop\n" +
                "This is the method beforeClose\n" +
                "This is the method afterClose\n", listener.getLog());
    }

    @Test
    public void testLifecycle5() throws Exception {
        //This test try to stop closed components

        validSetUp();

        lifecycleComponentStack.start();

        lifecycleComponentStack.close();

        //Closed components cannot change their state, so an exception is expected.
        boolean condition = false;
        try {
            lifecycleComponentStack.stop();
        } catch (IllegalStateException e) {
            condition = true;
        }
        Assert.assertTrue(condition);

    }

    @Test
    public void testLifecycle6() throws Exception {
        //This test cover the remained cases

        validSetUp();

        //Starting started components
        lifecycleComponentStack.start();

        lifecycleComponentStack.start();

        //Stopping stopped components
        lifecycleComponentStack.stop();

        lifecycleComponentStack.stop();

        //Closing closed components
        lifecycleComponentStack.close();

        lifecycleComponentStack.close();

        Assert.assertEquals("This is the method beforeStart\n" +
                        "This is the method afterStart\n" +
                        "This is the method beforeStart\n" +
                        "This is the method afterStart\n" +
                        "This is the method beforeStop\n" +
                        "This is the method afterStop\n" +
                        "This is the method beforeStop\n" +
                        "This is the method afterStop\n" +
                        "This is the method beforeClose\n" +
                        "This is the method afterClose\n" +
                        "This is the method beforeClose\n" +
                        "This is the method afterClose\n"
                , listener.getLog());

    }

    @Test
    public void testLifecycle7() throws Exception {
        //This test cover the case in which one of the component is going to have a bad behaviour. One of the component, instead to go
        // in the STOPPED state, goes in the CLOSED state. Hence, when the stack asks to restart, there will be a failure.

        failSetUp2();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        try {
            lifecycleComponentStack.start();
        } catch (Exception e) {
            //Exception caught
        }

        Assert.assertEquals("This is the method beforeStart\n" +
                        "This is the method afterStart\n" +
                        "This is the method beforeStart\n" +
                        "This is the method afterStart\n" +
                        "This is the method beforeStart\n" +
                        "This is the method afterStart\n" +
                        "This is the method beforeStop\n" +
                        "This is the method beforeClose\n" +
                        "This is the method afterClose\n" +
                        "This is the method afterStop\n" +
                        "This is the method beforeStop\n" +
                        "This is the method afterStop\n" +
                        "This is the method beforeStop\n" +
                        "This is the method afterStop\n" +
                        "This is the method beforeStart\n" +
                        "This is the method afterStart\n" +
                        "This is the method beforeStart\n" +
                        "This is the method afterStart\n"
                , listener.getLog());

    }

}