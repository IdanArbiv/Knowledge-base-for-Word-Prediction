import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class Main {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 10;

    public static void main(String[]args){
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to aws");
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        System.out.println( "list cluster");
        System.out.println( emr.listClusters());

		// Step 1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://bucket163897429777/jars/step1/MapReduceProject.jar")
                .withMainClass("Step1");

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
		// Step 2
        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar("s3://bucket163897429777/jars/step2/MapReduceProject.jar")
                .withMainClass("Step2");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
		// Step 3
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar("s3://bucket163897429777/jars/step3/MapReduceProject.jar")
                .withMainClass("Step3");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
		//Step 4
        HadoopJarStepConfig step4 = new HadoopJarStepConfig()
                .withJar("s3://bucket163897429777/jars/step4/MapReduceProject.jar")
                .withMainClass("Step4");

        StepConfig stepConfig4 = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(step4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map reduce project")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4)
                .withLogUri("s3://bucket163897429777/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}

