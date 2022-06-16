package com.fanfan.flink.day02;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName: Example3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月16日 17时08分
 * @Version: v1.0
 * Flink执行作业时底层是按照有向无环图来执行的，ExecutionGraph有向无环图 代表了数据从数据源到输出的路线图
 * 将JobGraph提交到作业管理器，会生成ExecutionGraph，也就是将算子按照并行度拆分成多个并行子任务。
 * ExecutionGraph中的每个顶点都要占用一个线程
 * <p>
 * TODO 实现图的遍历
 */
public class Example3 {
    public static void main(String[] args) {
        // flink中最重要的数据结构是：HashMap,用hashmap实现一个有向无环图
        HashMap<String, ArrayList<String>> dag = new HashMap<>();

        ArrayList<String> ANeighbors = new ArrayList<>();
        ANeighbors.add("B");
        ANeighbors.add("C");
        dag.put("A", ANeighbors);

        ArrayList<String> BNeighbors = new ArrayList<>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        dag.put("B", BNeighbors);

        ArrayList<String> CNeighbors = new ArrayList<>();
        CNeighbors.add("D");
        CNeighbors.add("E");
        dag.put("C", CNeighbors);

        // 拓扑排序--图的遍历算法
        // 参数（有向无环图，起点，将要打印的路径字符串）
        topoLogicSort(dag, "A", "A");
    }

    private static void topoLogicSort(HashMap<String, ArrayList<String>> dag, String vertex, String result) {
        if (vertex.equals("D") || vertex.equals("E")) {
            System.out.println(result);
        } else {
            ArrayList<String> vertexNeighbors = dag.get(vertex);
            // 遍历vertex的所有指向的顶点
            for (String v : vertexNeighbors) {
                topoLogicSort(dag, v, result + "->" + v);
            }
        }
    }
}
