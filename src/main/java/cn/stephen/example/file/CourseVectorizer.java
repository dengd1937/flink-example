package cn.stephen.example.file;

import cn.stephen.example.datagen.Course;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class CourseVectorizer extends Vectorizer<Course> implements Serializable {
    public CourseVectorizer(String schema) {
        super(schema);
    }

    @Override
    public void vectorize(Course course, VectorizedRowBatch vectorizedRowBatch) throws IOException {
        LongColumnVector categoryIdColVector = (LongColumnVector) vectorizedRowBatch.cols[0];
        BytesColumnVector descriptionColVector = (BytesColumnVector) vectorizedRowBatch.cols[1];
        LongColumnVector idColVector = (LongColumnVector) vectorizedRowBatch.cols[2];
        BytesColumnVector ipColVector = (BytesColumnVector) vectorizedRowBatch.cols[3];
        BytesColumnVector nameColVector = (BytesColumnVector) vectorizedRowBatch.cols[4];
        DoubleColumnVector moneyColVector = (DoubleColumnVector) vectorizedRowBatch.cols[5];
        BytesColumnVector osColVector = (BytesColumnVector) vectorizedRowBatch.cols[6];
        LongColumnVector statusColVector = (LongColumnVector) vectorizedRowBatch.cols[7];
        LongColumnVector tsColVector = (LongColumnVector) vectorizedRowBatch.cols[8];

        int row = vectorizedRowBatch.size++;
        categoryIdColVector.vector[row] = course.getCategoryId();
        descriptionColVector.setVal(row, course.getDescription().getBytes(StandardCharsets.UTF_8));
        idColVector.vector[row] = course.getId();
        ipColVector.setVal(row, course.getIp().getBytes(StandardCharsets.UTF_8));
        nameColVector.setVal(row, course.getName().getBytes(StandardCharsets.UTF_8));
        moneyColVector.vector[row] = course.getMoney();
        osColVector.setVal(row, course.getOs().getBytes(StandardCharsets.UTF_8));
        statusColVector.vector[row] = course.getStatus();
        tsColVector.vector[row] = course.getTs();
    }
}
