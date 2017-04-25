using System;
using System.Collections.Generic;
using System.Threading;

namespace ReadWriteLock {

    /*
     * 实现说明：
     *     1. 类说明：
     *        ReadWriteLock 是实现的读写锁
     *        RwLockTest 是读写锁的测试
     *     2. 使用：
     *        void function(){
     *             ReadWriteLock lockObj = new ReadWriteLock();
     *             lockObj.lockRead(); //获取读锁
     *             ... //读操作
     *             lockObj.unlockRead(); // 释放读锁
     *             
     *             lockObj.lockWrite(); //获取写锁
     *             ...//写操作
     *             lockObj.unlockWrite(); // 释放写锁
     *        }
     *     3. 功能：
     *        并发读
     *        多线程写，但写是互斥的
     *        读锁重入
     *        读锁升级写锁
     *        写锁重入
     *        写锁中申请读锁
     *     4. 实现
     *        - 判断逻辑主要在ReadWriteLock.canRead 和 ReadWriteLock.canWrite方法前的注释中， 读锁升级写锁和写锁请求
     *          读锁的使用条件以及异常出现的情况也在注释中
     *        - 因为要支持读锁的重入， 并且读是可以并发的，所以用一个字典保存了当前正在读的线程(key) 和 重入的次数(value)
     *        - 而由于写锁是不可并发的，只需要在记录当前写进程的数据结构里维护写锁重入数(writeCount)和写锁请求读锁数(readCount)
     *     5. 测试
     *        - 测试见RwLockTest测试中主函数的注释，一共分四种测试，输入和测试结果见相应代码部分的注释。
     * **/

    public class ReadWriteLock {

        /*这个结构体记录了当前正在写的线程， writeCount代表了写线程重入的次数， readCount代表了写的过程中请求读锁的数量*/
        class CurrentWritingThread {
            public Thread WritingThread;
            public int writeCount;
            public int readCount;
            public CurrentWritingThread(Thread thread) {
                WritingThread = thread;
                writeCount = 1;
                readCount = 0;
            }
        }

        /*
		 * 	mWritingThread是当前正在写的线程
		 *  mWriteRequests是正在请求写的线程队列, 用来实现写的顺序化
		 *  mReaderList是记录正在读的线程以及其持有读锁的数量，这个数据结构是用来实现读锁的重入的
		*/
        private CurrentWritingThread mWritingThread;
        private Queue<Thread> mWriteRequests;
        private Dictionary<Thread, int> mReaderDict;
        private Object mLock;



        public ReadWriteLock() {
            mWritingThread = null;
            mWriteRequests = new Queue<Thread>();
            mReaderDict = new Dictionary<Thread, int>();
            mLock = new object();
        }

        public void lockRead() {

            Monitor.Enter(mLock);
            try {
                Thread thisThread = Thread.CurrentThread;
                while (!canRead(thisThread)) { //如果当前无法读，等待
                    Monitor.Wait(mLock);
                };
                /*该线程拿到了读锁*/
                if (null == mWritingThread) { //当前写线程是空， 说明不是写线程申请的读锁
                    addReaderToReadingList(thisThread); // 把当前读线程加入读字典
                }
                else { // 当前写线程不是空， 说明是写线程申请了读锁
                    mWritingThread.readCount++; 
                }
                
            }
            catch (ThreadInterruptedException e) {
                throw e;
            }
            finally {
                Monitor.Exit(mLock); 
            }
        }

        public void unlockRead() {
            Monitor.Enter(mLock);

            try {
                Thread thisThread = Thread.CurrentThread;
                if (null == mWritingThread) { //当前写线程是空， 说明不是写线程申请的读锁
                    deleteReader(thisThread); //从读队列中删除
                }
                else {// 当前写线程不是空， 说明是写线程申请了读锁
                    mWritingThread.readCount--;
                    if (mWritingThread.readCount < 0) // 写线程的读者信号量小于0， 抛出异常
                        throw new ThreadInterruptedException();
                }
                
                Monitor.PulseAll(mLock); //唤醒其他所有的线程
            }
            catch (ThreadInterruptedException e) {
                throw e;
            }
            finally {
                Monitor.Exit(mLock);
            }
        }

        public void lockWrite() {
            Monitor.Enter(mLock);
            try {
                Thread thisThread = Thread.CurrentThread;
                /*如果没有写线程 或者 请求写的线程不是当前正在写的线程（不是写重入）， 就加入写队列*/
                if(mWritingThread == null || thisThread != mWritingThread.WritingThread)
                    mWriteRequests.Enqueue(thisThread);
                while (!canWrite(thisThread)) { //如果当前线程无法写，等待
                    Monitor.Wait(mLock);
                }

                if (null == mWritingThread) { //不是写重入
                    mWritingThread = new CurrentWritingThread(thisThread);
                    mWriteRequests.Dequeue();//从写请求队列中删除
                }
                else {//写重入
                    mWritingThread.writeCount++;
                }
                    


            }
            catch (ThreadInterruptedException e) {
                throw e;
            }
            finally {
                Monitor.Exit(mLock);
            }


        }

        public void unlockWrite() {
            Monitor.Enter(mLock);
            try {
                Thread thisThread = Thread.CurrentThread;
                if (1 == mWritingThread.writeCount) { // 如果不是重入的写锁
                    mWritingThread = null; //把当前正在写的线程记为null
                }else {
                    mWritingThread.writeCount--; // 重入计数减一
                }
                
                Monitor.PulseAll(mLock); //唤醒所有线程
            }
            catch (ThreadInterruptedException e) {
                throw e;
            }
            finally {
                Monitor.Exit(mLock); 
            }


        }

        /*
         * 线程可以读的情况：
         *  1. 不重入的情况：
         *      - 写请求队列里没有写请求 并且没有正在写的线程
         *  2. 重入的情况：
         *      - 没有正在写的线程，并且读请求里面已经有了这个线程
         *  3. 写锁请求读锁：
         *      - 因为写线程是当前唯一的线程，因此其他线程不可能有读锁，所以可以直接给予读锁，该线程不记录在当前读队列中
         * **/
        private bool canRead(Thread thisThread) {

            if (mWritingThread != null && thisThread == mWritingThread.WritingThread) { //写线程申请读锁, 直接放行
                return true;
            }

            if (!this.mReaderDict.ContainsKey(thisThread)) { //不重入的情况
                return (0 == this.mWriteRequests.Count && null == mWritingThread)? true : false;
            }
            else { //重入的情况
                return null == mWritingThread ? true : false;
            }
        }

        /*
         * 向读字典中添加一个线程
         * **/
        private void addReaderToReadingList(Thread thisThread) {
            int thisThreadCount = mReaderDict.ContainsKey(thisThread) ? mReaderDict[thisThread] : 0;
            if (0 == thisThreadCount) { // 如果不是写重入
                this.mReaderDict.Add(thisThread, 1);
            }
            else { // 写重入
                mReaderDict[thisThread] = thisThreadCount + 1;
            }
        }

        /*
         * 从读字典中除去一个线程
         * **/
        private void deleteReader(Thread thisThread) {
            int thisThreadCount = mReaderDict[thisThread];
            if (1 == thisThreadCount) mReaderDict.Remove(thisThread);
            else mReaderDict[thisThread] =  thisThreadCount - 1;
        }

        /*
         * 线程可以写的情况：
         * 1. 单纯申请写锁 
         *   没有正在写的线程 && 正在读的队列为空  && 当前线程在写请求线程队列的头
         * 2. 读锁升级为写锁
         *   当前线程是唯一正在读的线程 ---- 当只有一个读线程， 没有写线程的时候（当前线程在写请求线程队列的头）， 读锁可以升级成写锁
         *   当前读线程是唯一的读线程而且没有写请求的时候，可以升级为写锁
         *   如果当前申请升级的读线程是唯一的读线程，但是有写请求线程， 则此时当前线程在写请求队列的末尾，而正在读的线程字典中
         *   也有当前线程（造成了其他写请求线程不能获得写锁）， 从而会造成死锁。 所以当读锁升级写锁失败时抛出异常。
         * 3. 写锁重入
         *   当前写线程写锁重入， 已经有写锁的线程自然可以再申请一个写锁。 新的写锁并不存入写请求队列，
         *   只在当前线程的结构体中的信号量加一。
         * **/
        private bool canWrite(Thread thisThread) {

            /*写锁重入，直接获得权限*/
            if ( mWritingThread!=null && thisThread == mWritingThread.WritingThread) return true;

            /*没有正在写的线程*/
            bool noWritingThread = null == mWritingThread;
            /*正在读的队列为空*/
            bool noReaderOrOnlyReader = 0 == mReaderDict.Count;
            /*当前线程是唯一正在读的线程*/
            bool onlyReader = (mReaderDict.Count == 1 && mReaderDict.ContainsKey(thisThread));
            /*当前线程在写请求线程队列的头*/
            bool currentThreadAtFirst =  thisThread == mWriteRequests.Peek();

            /*先判断是否是读锁升级写锁*/
            if (mReaderDict.ContainsKey(thisThread)) {
                if (onlyReader && currentThreadAtFirst) {
                    return true;
                }
                throw new ThreadInterruptedException();//读锁升级写锁失败， 抛出异常
            }

            /*单纯申请写锁*/
            return noWritingThread && noReaderOrOnlyReader && currentThreadAtFirst;
        }

        /*
         * 向写请求队列中加入一个写请求 
         * **/
        private void addWriteRequest(Thread thisThread) {
            mWriteRequests.Enqueue(thisThread);
        }
    }

    public class RwLockTest {

        private  ReadWriteLock rwLock = new ReadWriteLock();

        private void Reader() {
            try {
                rwLock.lockRead(); // 申请读锁

                for (int i = 0; i < 1000; i++) {
                    Console.WriteLine(Thread.CurrentThread.Name + "Reading....");
                }

                rwLock.lockRead(); // 读锁重入
                for (int i = 0; i < 500; i++) {
                    Console.WriteLine("ReenLock -" + Thread.CurrentThread.Name + "Reading....");
                }
                

            }
            catch (Exception e) {
                Console.WriteLine(e.Data);
            }
            finally {
                rwLock.unlockRead(); //重入锁的释放
                rwLock.unlockRead();//释放读锁
            }

        }
        private void Writer(object pVal) {
            try {
                rwLock.lockWrite(); // 申请写锁
                for (int i = 0; i < 1000; i++) {
                    Console.WriteLine(Thread.CurrentThread.Name + "Writing ...");
                }
            }
            catch (Exception e) {
            }
            finally {
                rwLock.unlockWrite(); // 释放写锁
            }


        }

        /*
         * 读者和写者的测试，包括了读锁的可重入测试。
         * 测试方法：
         *      Writer 和 Reader 两个函数每隔50ms交替调用一次， 在Reader里面测试读锁的重入
         * 输出：
         *      先进行三个Writer操作，切三个Writer是按照顺序进行写。
         *      Reader操作和重入的Reader操作是并发执行的，可以看到Reader的序号是交替的。
         * **/
        public static void ReaderAndWriterTest() {
            RwLockTest obj = new RwLockTest();
            Thread t;
            for (int i = 0; i < 6; i++) {
                bool readOrWrite = 0 == i % 2;
                t = readOrWrite ? new Thread(obj.Writer) : new Thread(obj.Reader);
                t.Name = readOrWrite ? i / 2 + "-Writer " : (i / 2) + "-Reader";
                if (readOrWrite) t.Start(i % 2);
                else t.Start();
                Thread.Sleep(50);
            }
        }

        /*
         * 读锁升级为写锁的测试：
         *  条件：
         *      只有一个读线程， 没有写线程和写请求
         * **/
        void ReaderUpdateToWriter() {
            try {
                rwLock.lockRead();
                for (int i = 0; i < 500; i++) {
                    Console.WriteLine(Thread.CurrentThread.Name + "Reading.....");
                }
                rwLock.lockWrite();
                for (int i = 0; i < 500; i++) {
                    Console.WriteLine(Thread.CurrentThread.Name + "Reader update to writer, writing.....");
                }
            }
            catch (ThreadInterruptedException e) {
                Console.WriteLine(e.Data);
            }
            finally {
                rwLock.unlockWrite();
                rwLock.unlockRead();
            }
        }

        public static void ReaderUpdateToWriterTest() {
            RwLockTest obj = new RwLockTest();
            Thread t = new Thread(obj.ReaderUpdateToWriter);

            t.Start();
        }

        /*
         * 写锁降级为读锁的测试：
         *    测试：
         *        三次循环， 每次循环先进行写锁降级为读， 再进行读和读重入操作（上面的Reader()函数）
         *    结果：
         *        先是三次（写 + 写中读）， 然后三次读（并发）， 然后读重入（并发）
         * **/
        void WriterRequestForReader() {
            try {
                rwLock.lockWrite();
                for (int i = 0; i < 500; i++) { // 写
                    Console.WriteLine(Thread.CurrentThread.Name + "Writing....");
                }
                rwLock.lockRead();
                for (int i = 0; i < 500; i++) { // 写锁中申请读锁
                    Console.WriteLine(Thread.CurrentThread.Name + "Writer to reader, reading....");
                }
            }
            catch (ThreadInterruptedException e) {
                Console.WriteLine(e.Data);
            }
            finally {
                rwLock.unlockRead();
                rwLock.unlockWrite();
            }
        }
        public static void WriterRequestForReaderTest() {
            RwLockTest obj = new RwLockTest();

            for (int i = 0; i < 3; i++) {
                /*写降级为读*/
                Thread t = new Thread(obj.WriterRequestForReader);
                t.Name = i + "-";
                t.Start();
                /*读和读重入*/
                t = new Thread(obj.Reader);
                t.Name = i + "-pure_reader-";
                t.Start();
            }
        }


        /*
         * 写锁重入：
         *   测试：
         *        三次循环， 每次循环先进行写锁重入， 再进行读和读重入操作（上面的Reader()函数）
         *    结果：
         *        先是三次（写 + 写重入）， 然后三次读（并发）， 然后读重入（并发）
         *    
         * **/
        void WriterReenlock() {
            try {
                rwLock.lockWrite();
                for (int i = 0; i < 500; i++) {
                    Console.WriteLine(Thread.CurrentThread.Name + "Writing....");
                }
                rwLock.lockWrite();
                for (int i = 0; i < 500; i++) {
                    Console.WriteLine(Thread.CurrentThread.Name + "Reenlock writing....");
                }
            }
            catch (ThreadInterruptedException e) {
            }
            finally {
                rwLock.unlockWrite();
                rwLock.unlockWrite();
            }
        }

        public static void WriterReenlockTest() {
            RwLockTest obj = new RwLockTest();

            for (int i = 0; i < 3; i++) {
                Thread t = new Thread(obj.WriterReenlock);
                t.Name = i + "-";
                t.Start();

                t = new Thread(obj.Reader);
                t.Name = i + "-reader-";
                t.Start();
            }
        }


        public static void Main(String[] argv) {

            /*读者和写者的测试，包括了读锁的可重入测试。*/
            //ReaderAndWriterTest();

            /*读锁升级为写锁的测试*/
            //ReaderUpdateToWriterTest();
            
            /*写锁降级为读锁的测试*/
            /*WriterRequestForReaderTest()*/;

            /*写锁重入*/
            //WriterReenlockTest();

            /*输入回车结束*/
            Console.Read();
        }
    }
}
