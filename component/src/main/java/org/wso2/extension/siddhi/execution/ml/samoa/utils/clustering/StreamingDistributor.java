package org.wso2.extension.siddhi.execution.ml.samoa.utils.clustering;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.IntOption;
import com.google.common.collect.ImmutableSet;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.clusterers.ClustreamClustererAdapter;
import org.apache.samoa.learners.clusterers.LocalClustererAdapter;
import org.apache.samoa.learners.clusterers.LocalClustererProcessor;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;

import java.util.Set;

/**
 * Created by Jayan on 3/20/2017.
 */
public class StreamingDistributor implements Learner, Configurable {

    private static final long serialVersionUID = 684111382631697031L;

    private Stream resultStream;

    private Instances dataset;

    public ClassOption learnerOption = new ClassOption("learner", 'l', "Clusterer to use.", LocalClustererAdapter.class,
            ClustreamClustererAdapter.class.getName());

    public IntOption paralellismOption = new IntOption("paralellismOption", 'P',
            "The paralellism level for concurrent processes", 2, 1, Integer.MAX_VALUE);
    public IntOption samplemOption = new IntOption("SampleFrequencyOption", 'F',
            "The sample frequency of local clusters", 1000, 100, Integer.MAX_VALUE);
    public IntOption intervalOption = new IntOption("IntervalOption", 'I',
            "The frequency of output cluster centers values", 1000, 100, Integer.MAX_VALUE);

    private TopologyBuilder builder;

    // private ClusteringDistributorProcessor distributorP;
    private LocalClustererProcessor learnerP;

    // private Stream distributorToLocalStream;
    private Stream localToGlobalStream;

    // private int parallelism;

    @Override
    public void init(TopologyBuilder builder, Instances dataset, int parallelism) {
        this.builder = builder;
        this.dataset = dataset;
        // this.parallelism = parallelism;
        this.setLayout();
    }

    protected void setLayout() {
        // Local Clustering
        learnerP = new LocalClustererProcessor();
        learnerP.setSampleFrequency(samplemOption.getValue());
        LocalClustererAdapter learner = (LocalClustererAdapter) this.learnerOption.getValue();
        learner.setDataset(this.dataset);
        learnerP.setLearner(learner);
        builder.addProcessor(learnerP, this.paralellismOption.getValue());
        localToGlobalStream = this.builder.createStream(learnerP);
        learnerP.setOutputStream(localToGlobalStream);

        // Global Clustering
        LocalClustererProcessor globalClusteringCombinerP = new LocalClustererProcessor();
        globalClusteringCombinerP.setSampleFrequency((intervalOption.getValue()/samplemOption.getValue())*100);
        LocalClustererAdapter globalLearner = (LocalClustererAdapter) this.learnerOption.getValue();
        globalLearner.setDataset(this.dataset);
        globalClusteringCombinerP.setLearner(learner);
        builder.addProcessor(globalClusteringCombinerP, 1);
        builder.connectInputAllStream(localToGlobalStream, globalClusteringCombinerP);

        // Output Stream
        resultStream = this.builder.createStream(globalClusteringCombinerP);
        globalClusteringCombinerP.setOutputStream(resultStream);
    }

    @Override
    public Processor getInputProcessor() {
        // return distributorP;
        return learnerP;
    }

    @Override
    public Set<Stream> getResultStreams() {
        Set<Stream> streams = ImmutableSet.of(this.resultStream);
        return streams;
    }
}
