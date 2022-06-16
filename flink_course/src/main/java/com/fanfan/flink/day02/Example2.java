package com.fanfan.flink.day02;

import org.codehaus.plexus.util.dag.DAG;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName: Example2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月15日 13时46分
 * @Version: v1.0
 */
public class Example2 {
    public static void main(String[] args) {
        // 实现一个有向无环图
        HashMap<String, ArrayList<String>> dag = new HashMap<>();

        // 添加
        // A -> B
        // A -> C
        ArrayList<String> ANeighbors = new ArrayList<>();
        ANeighbors.add("B");
        ANeighbors.add("C");
        dag.put("A",ANeighbors);

        // B -> D
        // B -> E
        ArrayList<String> BNeighbors = new ArrayList<>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        dag.put("B", BNeighbors);

        // C -> D
        // C -> E
        ArrayList<String> CNeighbors = new ArrayList<>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        dag.put("C", CNeighbors);

        // A->B->D
        // A->B->E
        // A->B->D
        // A->B->E

        // 拓扑排序
        topologicalSort(dag,"A","A");
    }

    public static void topologicalSort(HashMap<String, ArrayList<String>> dag, String vertex, String result) {
        if(vertex.equals("D") || vertex.equals("E")){
            System.out.println(result);
        }else{
            // 遍历vertex的所有指向的顶点
            for (String v : dag.get(vertex)){
                topologicalSort(dag,v,result + "->" + v);
            }
        }
    }
    // 拓扑排序
}
