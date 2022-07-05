package com.fanfan.flink.day02;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @ClassName: Example3_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 13时18分
 * @Version: v1.0
 * TODO 实现一个有向无环图
 */
public class Example3_1 {
    public static void main(String[] args) {
        // dag的数据结构
        HashMap<String, ArrayList<String>> dag = new HashMap<>();
        ArrayList<String> ANeighbors = new ArrayList<>();
        ANeighbors.add("B");
        ANeighbors.add("C");
        dag.put("A",ANeighbors);

        ArrayList<String> BNeighbors = new ArrayList<>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        dag.put("B",BNeighbors);

        ArrayList<String> CNeighbors = new ArrayList<>();
        CNeighbors.add("D");
        CNeighbors.add("E");
        dag.put("C",CNeighbors);

        // 实现图的遍历 拓扑排序
        topologicSort(dag, "A", "A");
    }

    public static void topologicSort(HashMap<String, ArrayList<String>> dag, String vertex, String result) {
        if (vertex.equals("D") || vertex.equals("E")) {
            System.out.println(result);
        } else {
            // vertex指向的顶点构成的集合
            ArrayList<String> vertexNeighbors = dag.get(vertex);
            // 遍历vertex的所有指向的顶点
            for (String v : vertexNeighbors) {
                topologicSort(dag, v, result + "->" + v);
            }
        }
    }
}
