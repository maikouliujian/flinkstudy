package flinks.rpctest;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RpcTest {

    private static final Time TIMEOUT = Time.seconds(10L);
    private static ActorSystem actorSystem = null;
    private static RpcService rpcService = null;

    // 定义通信协议
    public interface HelloGateway extends RpcGateway {
        String hello();
    }

    public interface HiGateway extends RpcGateway {
        String hi();
    }

    // 具体实现
    public static class HelloRpcEndpoint extends RpcEndpoint implements HelloGateway {
        protected HelloRpcEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public String hello() {
            return "hello";
        }
    }

    public static class HiRpcEndpoint extends RpcEndpoint implements HiGateway {
        protected HiRpcEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public String hi() {
            return "hi";
        }
    }

    @BeforeClass
    public static void setup() {
        actorSystem = AkkaUtils.createDefaultActorSystem();
        // 创建 RpcService， 基于 AKKA 的实现
        rpcService = new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());
    }

    @AfterClass
    public static void teardown() throws Exception {

        final CompletableFuture<Void> rpcTerminationFuture = rpcService.stopService();
        final CompletableFuture<Terminated> actorSystemTerminationFuture = FutureUtils.toJava(actorSystem.terminate());

        FutureUtils
                .waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
                .get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
    }

    @Test
    public void test() throws Exception {
        HelloRpcEndpoint helloEndpoint = new HelloRpcEndpoint(rpcService);
        HiRpcEndpoint hiEndpoint = new HiRpcEndpoint(rpcService);

        helloEndpoint.start();

        //TODO 获取本地代理对象
        //获取 endpoint 的 self gateway
        HelloGateway helloGateway = helloEndpoint.getSelfGateway(HelloGateway.class);
        String hello = helloGateway.hello();
        assertEquals("hello", hello);

        hiEndpoint.start();
        // 通过 endpoint 的地址获得代理
        //TODO 获取远端代理对象
        HiGateway hiGateway = rpcService.connect(hiEndpoint.getAddress(),HiGateway.class).get();
        String hi = hiGateway.hi();
        assertEquals("hi", hi);
    }
}
