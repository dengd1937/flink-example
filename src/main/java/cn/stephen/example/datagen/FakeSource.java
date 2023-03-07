package cn.stephen.example.datagen;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FakeSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;
    private Long interval = 0L;

    public FakeSource() {}

    public FakeSource(Long interval) {
        this.interval = interval;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        while (isRunning) {
            ctx.collect(CourseFake.toJson());
            if (interval != 0L) {
                Thread.sleep(interval);
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
