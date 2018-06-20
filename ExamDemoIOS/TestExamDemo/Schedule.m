//
//  Schedule.m
//  ExamDemo
//
//  Created by George She on 2018/6/8.
//  Copyright © 2018年 CMRead. All rights reserved.
//

#import "Schedule.h"
#import "ReturnCodeKeys.h"

#pragma  mark - Server
@interface Server: NSObject
@property (nonatomic, assign) int noteId;
@property (nonatomic, strong) NSMutableArray<TaskInfo *> *runningQueue;
@end

@implementation Server
- (instancetype)init {
    self = [super init];
    if (self) {
        self.runningQueue = [NSMutableArray array];
    }
    return self;
}
@end

#pragma  mark - Schedule
@interface Schedule()
@property (nonatomic, strong) NSMutableDictionary<NSString *,Server *> *servers; //服务器 <nodeId,server>
@property (nonatomic, strong) NSMutableArray<TaskInfo *> *waitingQueue;//挂起队列
@end

@implementation Schedule
-(int)clean{
    self.servers = [NSMutableDictionary dictionary];
    self.waitingQueue = [NSMutableArray array];
    return kE001; //初始化成功
}

-(int)registerNode:(int)nodeId{
    if (nodeId < 0) {
        return kE004; //E004:服务节点编号非法
    }
    __block Server *targetServer;
    [self.servers enumerateKeysAndObjectsUsingBlock:^(NSString * _Nonnull key, Server * _Nonnull obj, BOOL * _Nonnull stop) {
        if (key.intValue == nodeId) {
            targetServer = obj;
            *stop = YES;
        }
    }];
    if (targetServer) {
        return kE005;//返回E005:服务节点已注册
    } else {
        targetServer = [Server new];
        targetServer.noteId = nodeId;
        [self.servers setObject:targetServer forKey:@(nodeId).stringValue];
        return kE003; //返回E003:服务节点注册成功
    }
    
}

-(int)unregisterNode:(int)nodeId{
    if (nodeId < 0) {
        return kE004; //E004:服务节点编号非法
    }
    __block Server *targetServer;
    [self.servers enumerateKeysAndObjectsUsingBlock:^(NSString * _Nonnull key, Server * _Nonnull obj, BOOL * _Nonnull stop) {
        if (key.intValue == nodeId) {
            targetServer = obj;
            *stop = YES;
        }
    }];
    
    if (!targetServer) {
        return kE007; //E007:服务节点不存在
    } else {
        if (targetServer.runningQueue.count > 0) {
            [targetServer.runningQueue enumerateObjectsUsingBlock:^(TaskInfo * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
                obj.nodeId = -1;
                [self.waitingQueue addObject:obj]; //如果该服务节点正运行任务，则将运行的任务移到任务挂起队列中，等待调度程序调度
            }];
        }
        [self.servers removeObjectForKey:@(nodeId).stringValue];
        return kE006; //E006:服务节点注销成功
    }
}

-(int)addTask:(int)taskId withConsumption:(int)consumption{
    if (taskId <= 0) {
        return kE009;//任务编号非法
    }
    __block TaskInfo *taskInfo;
    [self.waitingQueue enumerateObjectsUsingBlock:^(TaskInfo * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
        if (obj.taskId == taskId) {
            taskInfo = obj;
            *stop = YES;
        }
    }];
    if (taskInfo) {
        return kE010;//任务已添加
    } else {
        taskInfo = [TaskInfo new];
        taskInfo.taskId = taskId;
        taskInfo.nodeId = -1; //挂起队列
        taskInfo.consumption = consumption;
        [self.waitingQueue addObject:taskInfo];
        return kE008;//任务添加成功
    }
}

-(int)deleteTask:(int)taskId{
    if (taskId < 0) {
        return kE009;//任务编号非法
    }
    TaskInfo *taskInfo = [self queryTaskWithTaskId:taskId];
    if (!taskInfo) {
        return kE012;//任务不存在
    } else {
        if (taskInfo.nodeId == -1) {
            [self.waitingQueue removeObject:taskInfo]; //挂起队列中的任务删除。
        } else {
            Server *server = [self.servers objectForKey:@(taskInfo.nodeId).stringValue];
            if (server) {
                [server.runningQueue removeObject:taskInfo];  //运行在服务节点上的任务删除。
            } else {
                NSLog(@"nodeId 不存在");
            }
        }
        return kE011;//任务删除成功
    }
}

-(int)scheduleTask:(int)threshold{
    if (threshold <= 0) {
        return kE002;//调度阈值非法
    }
    NSMutableArray<TaskInfo *>  *tasks = [NSMutableArray array];
    [self queryTaskStatus:tasks];
    [tasks sortUsingComparator:^NSComparisonResult(TaskInfo  *_Nonnull obj1, TaskInfo *_Nonnull obj2) {
        if (obj1.consumption < obj2.consumption) {
            return NSOrderedAscending;
        }
        return NSOrderedDescending;
    }];
    __block int totalConsumption = 0;
    [tasks enumerateObjectsUsingBlock:^(TaskInfo * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
        totalConsumption += obj.consumption ;
    }];
    int mean = totalConsumption/self.servers.count;
    int remainder = totalConsumption%self.servers.count;
    int maxValue = mean+threshold/2;
    int minValue = maxValue - threshold;
    __block currentSum = 0;
    __block NSMutableArray *baseArray = [NSMutableArray array];
    int minIndex;
    for (int i = 0; i < tasks.count; i++) {
        TaskInfo *obj = tasks[i];
        if (currentSum < minValue) {
            currentSum += obj.consumption;
            [baseArray addObject:obj];
        }
    }
    
    if (remainder <= threshold) {
        
        
    } else {
        return kE014;//无合适迁移方案
    }
    
    return kE013;// 任务调度成功
}

-(int)queryTaskStatus:(NSMutableArray<TaskInfo *> *)tasks {
    [self.servers enumerateKeysAndObjectsUsingBlock:^(NSString * _Nonnull key, Server * _Nonnull obj, BOOL * _Nonnull stop) {
        [tasks addObjectsFromArray:obj.runningQueue];
    }];
    [tasks addObjectsFromArray:self.waitingQueue];
    if (tasks.count > 0) {
        return kE015;  //查询任务状态成功
    } else {
        return kE016; //E016:参数列表非法
    }
}

#pragma mark - Private
- (TaskInfo *)queryTaskWithTaskId:(int)taskId {
    __block TaskInfo *taskInfo;
    NSMutableArray<TaskInfo *> *totalTaskList = [NSMutableArray arrayWithArray:self.waitingQueue];
    [self.servers enumerateKeysAndObjectsUsingBlock:^(NSString * _Nonnull key, Server * _Nonnull obj, BOOL * _Nonnull stop) {
        [totalTaskList addObjectsFromArray:obj.runningQueue];
    }];
    [totalTaskList enumerateObjectsUsingBlock:^(TaskInfo * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
        if (obj.taskId == taskId) {
            taskInfo = obj;
            *stop = YES;
        }
    }];
    return taskInfo;
}

@end
