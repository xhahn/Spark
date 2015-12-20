package com.xhahn.gmm.Chameleon;

import java.util.ArrayList;

/**
 * 聚簇类
 *
 * @author lyq
 */
public class Cluster implements Cloneable {
    //簇唯一id标识号
    int id;
    // 聚簇内的坐标点集合
    ArrayList<Point> points;
    // 聚簇内的所有边的权重和
    double weightSum = 0;

    public Cluster(int id, ArrayList<Point> points) {
        this.id = id;
        this.points = points;
    }

    /**
     * 计算聚簇的内部的边权重和
     *
     * @return double
     */
    public double calEC() {
        int id1;
        int id2;
        weightSum = 0;

        for (Point p1 : points) {
            for (Point p2 : points) {
                id1 = p1.id;
                id2 = p2.id;

                // 为了避免重复计算，取id1小的对应大的
                if (id1 < id2 && ChameleonTool.edges[id1][id2] == 1) {
                    weightSum += ChameleonTool.weights[id1][id2];
                }
            }
        }

        return weightSum;
    }

    /**
     * 计算2个簇之间最近的n条边
     *
     * @param otherCluster 待比较的簇
     * @param n            最近的边的数目
     * @return ArrayList<int[]>
     */
    public ArrayList<int[]> calNearestEdge(Cluster otherCluster, int n) {
        double distance;
        double minDistance;
        Point point1 = null;
        Point point2 = null;
        ArrayList<int[]> edgeList = new ArrayList<int[]>();
        ArrayList<Point> pointList1 = (ArrayList<Point>) points.clone();
        ArrayList<Point> pointList2 = null;
        Cluster c2;

        try {
            c2 = (Cluster) otherCluster.clone();
            pointList2 = c2.points;
        } catch (CloneNotSupportedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        int[] tempEdge;
        while (pointList1.size() != 0 && pointList2.size() != 0) {
            tempEdge = new int[2];
            minDistance = Integer.MAX_VALUE;

            for (Point p1 : pointList1) {
                for (Point p2 : pointList2) {
                    distance = p1.ouDistance(p2);
                    if (distance < minDistance) {
                        point1 = p1;
                        point2 = p2;
                        tempEdge[0] = p1.id;
                        tempEdge[1] = p2.id;

                        minDistance = distance;
                    }
                }
            }

            pointList1.remove(point1);
            pointList2.remove(point2);
            edgeList.add(tempEdge);
        }

        return edgeList;
    }

    public ArrayList<int[]> calNearestEdge(Cluster otherCluster) {
        double distance;
        double minDistance;
        Point point1 = null;
        Point point2 = null;
        ArrayList<int[]> edgeList = new ArrayList<int[]>();
        ArrayList<Point> pointList1 = (ArrayList<Point>) points.clone();
        ArrayList<Point> pointList2 = null;
        Cluster c2;

        try {
            c2 = (Cluster) otherCluster.clone();
            pointList2 = c2.points;
        } catch (CloneNotSupportedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        int[] tempEdge;
        while (pointList1.size() != 0 && pointList2.size() != 0) {
            tempEdge = new int[2];
            minDistance = Integer.MAX_VALUE;

            for (Point p1 : pointList1) {
                for (Point p2 : pointList2) {
                    distance = p1.ouDistance(p2);
                    if (distance < minDistance) {
                        point1 = p1;
                        point2 = p2;
                        tempEdge[0] = p1.id;
                        tempEdge[1] = p2.id;

                        minDistance = distance;
                    }
                }
            }

            pointList1.remove(point1);
            pointList2.remove(point2);
            edgeList.add(tempEdge);
        }

        return edgeList;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        // TODO Auto-generated method stub

        //引用需要再次复制，实现深拷贝
        ArrayList<Point> pointList = (ArrayList<Point>) this.points.clone();
        Cluster cluster = new Cluster(id, pointList);

        return cluster;
    }


}
