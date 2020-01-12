package com.trendcore.learning.apache.spark;


import de.bechte.junit.runners.context.HierarchicalContextRunner;
import org.junit.*;
import org.junit.runner.RunWith;

@RunWith(HierarchicalContextRunner.class)
public class MainTest {

    @BeforeClass
    public static void runsOnlyOnce_Before_AllTestsInMainTest() throws Exception {
        System.out.println("runsOnlyOnce_Before_AllTestsInMainTest");
    }

    @Test
    public void simpleTestInTheMainContext() throws Exception {
        System.out.println("simpleTestInTheMainContext");
        Assert.assertTrue(true);
    }

    @Test
    public void anotherTestInTheMainContext() throws Exception {
        System.out.println("anotherTestInTheMainContext");
        Assert.assertTrue(true);
    }

    public class SubContext {
        @Before
        public void runs_Before_EachTestWithinSubContextAndAllContainingContexts() {
            System.out.println("runs_Before_EachTestWithinSubContextAndAllContainingContexts");
        }

        @After
        public void runs_After_EachTestWithinSubContextAndAllContainingContexts() {
            System.out.println("runs_After_EachTestWithinSubContextAndAllContainingContexts");
        }

        @Test
        public void simpleTestInTheSubContext() throws Exception {
            System.out.println("simpleTestInTheSubContext");
        }
        //  more tests in the sub context
    }

    public class SubSubContext1 {
        @Before
        public void runs_Before_EachTestWithinSubSubContext1AndAllContainingContexts() {
            System.out.println("runs_Before_EachTestWithinSubSubContext1AndAllContainingContexts");
        }

        @After
        public void runs_After_EachTestWithinSubSubContext1AndAllContainingContexts() {
            System.out.println("runs_After_EachTestWithinSubSubContext1AndAllContainingContexts");
        }

        @Test
        public void simpleTestInTheSubSubContext1() throws Exception {
            System.out.println("simpleTestInTheSubSubContext1");
        }

        //  more tests in the sub sub context 1 
    }

    public class SubSubContext2 {
        @Before
        public void runs_Before_EachTestWithinSubSubContext2AndAllContainingContexts() {
            System.out.println("runs_Before_EachTestWithinSubSubContext2AndAllContainingContexts");
        }

        @After
        public void runs_After_EachTestWithinSubSubContext2AndAllContainingContexts() {
            System.out.println("runs_After_EachTestWithinSubSubContext2AndAllContainingContexts");
        }

        @Test
        public void simpleTestInTheSubSubContext2() throws Exception {
            System.out.println("simpleTestInTheSubSubContext2");
        }

        //  more tests in the sub sub context 2 
    }

    @AfterClass
    public static void runsOnlyOnce_After_AllTestsInMainTest() throws Exception {
        System.out.println("runsOnlyOnce_After_AllTestsInMainTest");
    }

}