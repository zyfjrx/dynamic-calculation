package com.byt.tagcalculate.iotdb.source;//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IoTDBSource<T> extends RichSourceFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBSource.class);
    private static final long serialVersionUID = 1L;
    private IoTDBSourceOptions sourceOptions;
    private transient Session session;
    private transient SessionDataSet dataSet;

    protected IoTDBSource(IoTDBSourceOptions ioTDBSourceOptions) {
        this.sourceOptions = ioTDBSourceOptions;
    }

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.initSession();
    }

    public abstract T convert(RowRecord var1);

    public void run(SourceFunction.SourceContext<T> sourceContext) throws Exception {
        this.dataSet = this.session.executeQueryStatement(this.sourceOptions.getSql());
        this.dataSet.setFetchSize(this.sourceOptions.getFetchSize());

        while(this.dataSet.hasNext()) {
            sourceContext.collect(this.convert(this.dataSet.next()));
        }

        this.dataSet.closeOperationHandle();
    }

    public void cancel() {
        try {
            this.dataSet.closeOperationHandle();
        } catch (IoTDBConnectionException | StatementExecutionException var2) {
            LOG.error(var2.getMessage());
        }

    }

    public void close() throws Exception {
        super.close();

        try {
            this.dataSet.closeOperationHandle();
        } catch (IoTDBConnectionException | StatementExecutionException var5) {
            throw var5;
        } finally {
            this.session.close();
        }

    }

    void initSession() throws IoTDBConnectionException {
        this.session = new Session(this.sourceOptions.getHost(), this.sourceOptions.getPort(), this.sourceOptions.getUser(), this.sourceOptions.getPassword());
        this.session.open();
    }
}
