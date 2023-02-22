package cn.stephen.example.datagen;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FakeSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        while (isRunning) {
            ctx.collect(CourseFake.toJson());
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
