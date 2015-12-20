package com.xhahn.gmm.Chameleon;

/**
 * 坐标点类
 *
 * @author lyq
 */
public class Point {
    //坐标点id号,id号唯一
    int id;
    //坐标横坐标
    Integer[] Coordinates = new Integer[12];
    String user_id;

    //是否已经被访问过
    boolean isVisited;

    public Point(String[] Coordinates) {
        user_id = Coordinates[0];
        for(int i=0;i<12;i++){
            this.Coordinates[i] = Integer.parseInt(Coordinates[i+1]);
        }
    }

    /**
     * 计算当前点与制定点之间的欧式距离
     *
     * @param p 待计算聚类的p点
     * @return
     */
    public double ouDistance(Point p) {
        double distance = 0;

        for (int i=0;i<12;i++){
        distance = (this.Coordinates[i]-p.Coordinates[i])*(this.Coordinates[i]-p.Coordinates[i]);
        }
        distance = Math.pow(distance,(double)1/12);

        return distance;
    }

    /**
     * 判断2个坐标点是否为用个坐标点
     *
     * @param p 待比较坐标点
     * @return
     */
    public boolean isTheSame(Point p) {
        boolean isSamed = true;

        for (int i=0;i<12;i++) {
            if (this.Coordinates[i] != p.Coordinates[i]) {
                isSamed = false;
                break;
            }
        }
        return isSamed;
    }
}
