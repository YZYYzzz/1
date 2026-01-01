const WebSocket = require('ws');
const http = require('http');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');
const fs = require('fs');
const path = require('path');

// 配置参数
const CONFIG = {
    // ========== 服务器配置 ==========
    HOST: process.env.HOST || 'localhost',
    HOST_PORT: parseInt(process.env.HOST_PORT) || 8084,
    CLIENT_PORT: parseInt(process.env.CLIENT_PORT) || 9191,
    
    // ========== 性能配置 ==========
    WORKER_COUNT: Math.max(1, os.cpus().length - 1), // 工作线程数(CPU核心数-1，至少保留1个)
    MESSAGE_BATCH_SIZE: parseInt(process.env.MESSAGE_BATCH_SIZE) || 10,      // 每批次处理的消息数量
    MESSAGE_BATCH_INTERVAL: parseInt(process.env.MESSAGE_BATCH_INTERVAL) || 10,  // 批处理间隔(毫秒)
    
    // ========== 安全配置 ==========
    MAX_CLIENTS_PER_KEY: parseInt(process.env.MAX_CLIENTS_PER_KEY) || 5,     // 每个key最大允许的客户端数量
    RATE_LIMIT_WINDOW: 60 * 1000, // 限流时间窗口(毫秒)
    RATE_LIMIT_MAX_REQUESTS: 100,  // 时间窗口内最大请求数
    
    // ========== 超时配置 ==========
    CLIENT_TIMEOUT: 30 * 1000,    // 客户端超时时间(毫秒)
    HEARTBEAT_INTERVAL: 10 * 1000, // 心跳间隔(毫秒)
    
    // ========== 内存管理配置 ==========
    MEMORY_CHECK_INTERVAL: 60 * 1000, // 内存检查间隔(毫秒)
    MEMORY_THRESHOLD: 0.85,           // 内存使用率阈值(85%)
    GC_INTERVAL: 30 * 1000,           // 强制GC间隔(开发环境)
    
    // ========== 连接管理配置 ==========
    MAX_QUEUE_SIZE_PER_KEY: 1000,     // 每个key最大队列大小
    CLIENT_CLEANUP_INTERVAL: 30 * 1000, // 客户端清理间隔
    
    // ========== 日志配置 ==========
    LOG_LEVEL: process.env.LOG_LEVEL || 'info', // debug, info, warn, error
    ENABLE_ACCESS_LOG: process.env.ENABLE_ACCESS_LOG !== 'false'
};

// 环境检查
if (CONFIG.WORKER_COUNT >= os.cpus().length) {
    console.warn(`警告: WORKER_COUNT(${CONFIG.WORKER_COUNT}) 等于或超过CPU核心数(${os.cpus().length})，可能影响系统性能`);
}

// 配置验证
if (CONFIG.MESSAGE_BATCH_SIZE <= 0) {
    throw new Error('MESSAGE_BATCH_SIZE 必须大于0');
}

module.exports = CONFIG;

// 全局输出控制：启用控制台输出（保留去重逻辑可选）
(function setupLogging() {
    const SILENCE_OUTPUT = false; // 设为 true 屏蔽输出
    if (SILENCE_OUTPUT) {
        console.log = function(){};
        console.info = function(){};
        console.warn = function(){};
        console.error = function(){};
        return;
    }
    // 如需开启并做去重，可改为 true 启用下方去重逻辑
    const ENABLE_LOG_DEDUP = false;
    const WINDOW_MS = 5000;
    if (!ENABLE_LOG_DEDUP) return;
    const origLog = console.log.bind(console);
    const lastTime = new Map();
    console.log = (...args) => {
        try {
            const key = args.map(a => (typeof a === 'string' ? a : JSON.stringify(a))).join(' | ');
            const now = Date.now();
            const prev = lastTime.get(key) || 0;
            if (now - prev < WINDOW_MS) return;
            lastTime.set(key, now);
            origLog(...args);
        } catch (e) {
            origLog(...args);
        }
    };
})();

// 主线程代码
if (isMainThread) {
    // 内存监控
    let memoryUsageHistory = [];
    const MAX_MEMORY_HISTORY = 100;

    // 定期内存检查
    function checkMemoryUsage() {
        const memUsage = process.memoryUsage();
        const usageRatio = memUsage.heapUsed / memUsage.heapTotal;
        
        memoryUsageHistory.push({
            timestamp: Date.now(),
            heapUsed: memUsage.heapUsed,
            heapTotal: memUsage.heapTotal,
            external: memUsage.external,
            arrayBuffers: memUsage.arrayBuffers
        });
        
        // 保持历史记录大小
        if (memoryUsageHistory.length > MAX_MEMORY_HISTORY) {
            memoryUsageHistory = memoryUsageHistory.slice(-MAX_MEMORY_HISTORY);
        }
        
        // 内存使用率超过阈值时触发清理
        if (usageRatio > CONFIG.MEMORY_THRESHOLD) {
            console.warn(`内存使用率过高: ${(usageRatio * 100).toFixed(2)}%，触发紧急清理`);
            emergencyCleanup();
        }
        
        // 打印内存状态（每分钟）
        if (memoryUsageHistory.length % 10 === 0) {
            console.log(`内存状态: 使用 ${(memUsage.heapUsed / 1024 / 1024).toFixed(2)}MB / 总量 ${(memUsage.heapTotal / 1024 / 1024).toFixed(2)}MB (${(usageRatio * 100).toFixed(2)}%)`);
        }
    }

    // 紧急内存清理
    function emergencyCleanup() {
        console.log('执行紧急内存清理...');
        
        // 1. 清理过大的消息队列
        let totalCleared = 0;
        for (const [key, queue] of messageQueues.entries()) {
            if (queue.length > CONFIG.MAX_QUEUE_SIZE_PER_KEY) {
                const originalSize = queue.length;
                const excess = originalSize - CONFIG.MAX_QUEUE_SIZE_PER_KEY;
                queue.splice(0, excess); // 移除最早的消息
                totalCleared += excess;
                console.warn(`清理key=${key}的消息队列: ${originalSize} -> ${queue.length}`);
            }
        }
        
        // 2. 清理僵尸客户端连接
        let zombieClients = 0;
        for (const [key, clients] of clientsMap.entries()) {
            const activeClients = new Set();
            clients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    activeClients.add(client);
                } else {
                    zombieClients++;
                }
            });
            clientsMap.set(key, activeClients);
            
            // 如果该key没有活跃客户端，清理相关资源
            if (activeClients.size === 0 && !activeHostKeys.has(key)) {
                clientsMap.delete(key);
                messageQueues.delete(key);
                batchProcessingStatus.delete(key);
            }
        }
        
        // 3. 强制垃圾回收（仅在Node.js启动时添加--expose-gc参数时可用）
        if (global.gc) {
            try {
                global.gc();
                console.log('强制垃圾回收完成');
            } catch (e) {
                console.log('强制垃圾回收失败:', e.message);
            }
        }
        
        console.log(`紧急清理完成: 清除${totalCleared}条队列消息, ${zombieClients}个僵尸客户端`);
    }

    // 定期连接清理
    function cleanupStaleConnections() {
        const now = Date.now();
        let cleanedClients = 0;
        let cleanedQueues = 0;
        
        // 清理长时间未处理的消息队列
        for (const [key, queue] of messageQueues.entries()) {
            if (queue.length > 0 && !activeHostKeys.has(key)) {
                // 如果该key没有活跃的主用户，但队列中有消息，清理队列
                const queueSize = queue.length;
                messageQueues.delete(key);
                batchProcessingStatus.delete(key);
                cleanedQueues++;
                console.log(`清理无效队列 key=${key}, 消息数=${queueSize}`);
            }
        }
        
        // 清理没有客户端的key
        for (const [key, clients] of clientsMap.entries()) {
            if (clients.size === 0 && !activeHostKeys.has(key)) {
                clientsMap.delete(key);
                cleanedClients++;
            }
        }
        
        if (cleanedClients > 0 || cleanedQueues > 0) {
            console.log(`定期清理: ${cleanedClients}个空客户端组, ${cleanedQueues}个无效队列`);
        }
    }

    // 创建两个HTTP服务器，用于获取客户端IP
    const hostHttpServer = http.createServer((req, res) => {
        // 根据文件类型设置不同的缓存策略
        setCacheHeaders(req, res);
        
        // 处理静态文件请求
        if (req.method === 'GET' && !req.url.includes('?')) {
            handleStaticFile(req, res);
        } else {
            res.writeHead(404);
            res.end('Not Found');
        }
    });
    
    const clientHttpServer = http.createServer((req, res) => {
        // 根据文件类型设置不同的缓存策略
        setCacheHeaders(req, res);
        
        // 处理静态文件请求
        if (req.method === 'GET' && !req.url.includes('?')) {
            handleStaticFile(req, res);
        } else {
            res.writeHead(404);
            res.end('Not Found');
        }
    });
    
    // 根据文件类型设置缓存头
    function setCacheHeaders(req, res) {
        const url = req.url || '';
        const extname = path.extname(url);
        
        // 为图标文件设置较短的缓存时间，防止缓存错乱
        if (url.includes('/Radar/') || url.includes('/agent/') || url.includes('/maps/') || 
            ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico'].includes(extname)) {
            // 图标文件：短缓存 + ETag验证
            res.setHeader('Cache-Control', 'public, max-age=300, must-revalidate'); // 5分钟缓存
            res.setHeader('ETag', '"' + Date.now().toString(36) + '"'); // 添加ETag
        } else if (['.htm', '.html'].includes(extname)) {
            // HTML文件：无缓存，始终验证
            res.setHeader('Cache-Control', 'no-cache, must-revalidate');
            res.setHeader('Pragma', 'no-cache');
        } else if (['.js', '.css'].includes(extname)) {
            // JS/CSS文件：中等缓存
            res.setHeader('Cache-Control', 'public, max-age=3600'); // 1小时缓存
        } else {
            // 其他文件：默认缓存
            res.setHeader('Cache-Control', 'public, max-age=86400'); // 1天缓存
        }
    }

    // 处理静态文件
    function handleStaticFile(req, res) {
        let filePath = '.' + req.url;
        if (filePath === './') {
            filePath = './ld.htm';
        }
        
        const extname = path.extname(filePath);
        const contentType = {
            '.htm': 'text/html',
            '.html': 'text/html',
            '.js': 'text/javascript',
            '.css': 'text/css',
            '.json': 'application/json',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.gif': 'image/gif',
            '.svg': 'image/svg+xml',
            '.ico': 'image/x-icon'
        }[extname] || 'text/plain';
        
        fs.readFile(filePath, (error, content) => {
            if (error) {
                if (error.code === 'ENOENT') {
                    res.writeHead(404);
                    res.end('File not found');
                } else {
                    res.writeHead(500);
                    res.end('Server Error: ' + error.code);
                }
            } else {
                res.writeHead(200, { 'Content-Type': contentType });
                res.end(content, 'utf-8');
            }
        });
    }

    // 创建两个WebSocket服务器
    const hostServer = new WebSocket.Server({ server: hostHttpServer, perMessageDeflate: {
            zlibDeflateOptions: { level: 6 },
            zlibInflateOptions: { chunkSize: 16 * 1024 },
            clientNoContextTakeover: false,
            serverNoContextTakeover: false,
            serverMaxWindowBits: 15,
            concurrencyLimit: 10,
            threshold: 256
        } });
    const clientServer = new WebSocket.Server({ server: clientHttpServer, perMessageDeflate: {
            zlibDeflateOptions: { level: 6 },
            zlibInflateOptions: { chunkSize: 16 * 1024 },
            clientNoContextTakeover: false,
            serverNoContextTakeover: false,
            serverMaxWindowBits: 15,
            concurrencyLimit: 10,
            threshold: 256
        } });

    console.log(`WebSocket服务器启动 (${CONFIG.WORKER_COUNT}个工作线程):`);
    console.log(`主用户服务器监听端口: ${CONFIG.HOST_PORT}`);
    console.log(`客户端服务器监听端口: ${CONFIG.CLIENT_PORT}`);

    // 存储不同key的客户端连接
    const clientsMap = new Map(); // key -> Set<WebSocket>

    // 存储活跃的主用户key
    const activeHostKeys = new Set(); // 记录当前已连接的主用户key

    // 已移除IP封禁列表机制

    // 为每个key维护一个消息队列和批处理状态
    const messageQueues = new Map(); // key -> Array<messages>
    const batchProcessingStatus = new Map(); // key -> {processing: boolean, batchId: number}

    // 创建工作线程池 - 添加健康检查
    const workerPool = [];
    function createWorker(workerId) {
        const worker = new Worker(__filename, {
            workerData: { workerId }
        });
        
        worker.exited = false;
        worker.workerId = workerId;
        
        // 处理工作线程发送的消息
        worker.on('message', ({ key, data, batchId }) => {
            // 如果是转换后的JSON，打印一条简短示例日志
            try {
                if (typeof data === 'string' && data.includes('"coords":"map"')) {
                    const obj = JSON.parse(data);
                    if (obj && obj.coords === 'map' && Array.isArray(obj.players)) {
                       // const sample = obj.players.slice(0, 3).map(p => ({ id: p.id, x: p.x, y: p.y }));
                       // console.log(`[Send] 转换坐标 -> key=${key}, players=${obj.players.length}, 示例:`, sample);
                    }
                }
            } catch (_) {}
            // 获取对应key的客户端集合
            const clients = clientsMap.get(key);
            if (clients && clients.size > 0) {
                // 批量转发数据给所有使用相同key的客户端
                clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) {
                        try {
                            client.send(data);
                        } catch (sendErr) {
                            console.error(`工作线程${workerId}：发送数据失败 [批次${batchId}]`);
                        }
                    }
                });
            }
        });
        
        worker.on('error', (err) => {
            console.error(`工作线程${workerId}错误:`, err);
            worker.exited = true;
            // 延迟重新创建工作线程，避免频繁重启
            setTimeout(() => {
                console.log(`重新创建工作线程${workerId}`);
                workerPool[workerId] = createWorker(workerId);
            }, 5000);
        });
        
        worker.on('exit', (code) => {
            console.log(`工作线程${workerId}退出，代码: ${code}`);
            worker.exited = true;
            if (code !== 0) {
                setTimeout(() => {
                    console.log(`重新创建工作线程${workerId}`);
                    workerPool[workerId] = createWorker(workerId);
                }, 5000);
            }
        });
        
        return worker;
    }

    for (let i = 0; i < CONFIG.WORKER_COUNT; i++) {
        workerPool.push(createWorker(i));
    }

    // 为同一 key 选择稳定的工作线程索引，避免跨线程导致状态丢失
    function getWorkerIndexForKey(key) {
        try {
            let hash = 0;
            for (let i = 0; i < key.length; i++) {
                hash = ((hash << 5) - hash + key.charCodeAt(i)) | 0; // 32位整数哈希
            }
            return Math.abs(hash) % CONFIG.WORKER_COUNT;
        } catch (_) {
            return 0; // 兜底
        }
    }

    // 批量处理消息队列
    function processBatch(key) {
        const status = batchProcessingStatus.get(key);
        if (!status || status.processing) return;
        
        const queue = messageQueues.get(key);
        if (!queue || queue.length === 0) {
            // 队列为空，重置状态
            status.processing = false;
            return;
        }
        
        // 标记为正在处理
        status.processing = true;
        
        // 取出一批消息
        const batch = queue.splice(0, CONFIG.MESSAGE_BATCH_SIZE);
        if (batch.length === 0) {
            status.processing = false;
            return;
        }
        
        // 检查工作线程是否健康
        const workerIndex = getWorkerIndexForKey(key);
        const worker = workerPool[workerIndex];
        
        if (!worker || worker.exited) {
            console.error(`工作线程${workerIndex}不可用，跳过处理`);
            status.processing = false;
            return;
        }
        
        status.batchId++;
        
        try {
            // 发送到工作线程处理
            worker.postMessage({
                key,
                batch,
                batchId: status.batchId
            });
        } catch (err) {
            console.error(`向工作线程发送消息失败:`, err);
            // 如果发送失败，将消息重新放回队列开头
            queue.unshift(...batch);
        }
        
        // 一段时间后处理下一批
        setTimeout(() => {
            status.processing = false;
            processBatch(key);
        }, CONFIG.MESSAGE_BATCH_INTERVAL);
    }

    // 添加消息到队列并触发批处理 - 增加队列大小限制
    function queueMessage(key, message) {
        // 确保该key有一个消息队列
        if (!messageQueues.has(key)) {
            messageQueues.set(key, []);
        }
        
        const queue = messageQueues.get(key);
        
        // 检查队列大小限制
        if (queue.length >= CONFIG.MAX_QUEUE_SIZE_PER_KEY) {
            console.warn(`消息队列已满 key=${key}, 丢弃最早的消息`);
            queue.shift(); // 移除最早的消息
        }
        
        // 确保该key有一个批处理状态
        if (!batchProcessingStatus.has(key)) {
            batchProcessingStatus.set(key, {
                processing: false,
                batchId: 0,
                lastActivity: Date.now()
            });
        } else {
            // 更新最后活动时间
            batchProcessingStatus.get(key).lastActivity = Date.now();
        }
        
        // 添加消息到队列
        queue.push(message);
        
        // 触发批处理
        processBatch(key);
    }

    // 处理主用户连接
    hostServer.on('connection', (ws, req) => {
        const key = getKeyFromUrl(req.url);
        if (!key) {
            ws.close(1008, '缺少key参数');
            return;
        }
        
        console.log(`主用户连接成功，KEY: ${key}`);
        
        // 记录活跃的主用户key
        activeHostKeys.add(key);
        console.log(`当前活跃的主用户KEY数量: ${activeHostKeys.size}`);
        
        // 确保这个key有一个客户端集合
        if (!clientsMap.has(key)) {
            clientsMap.set(key, new Set());
        }
        
        // 接收主用户发送的数据
        ws.on('message', (data) => {
            try {
                // 调试：打印主用户消息的类型与前缀
                try {
                    if (Buffer.isBuffer(data)) {
                        const previewHex = data.slice(0, 32).toString('hex');
                        const previewUtf8 = data.slice(0, 64).toString('utf8');
                        // console.log(`[HostMsg] 收到Buffer len=${data.length}, hex=${previewHex}, utf8=${previewUtf8}`);
                    } else if (typeof data === 'string') {
                        console.log(`[HostMsg] 收到String len=${data.length}, prefix=${data.slice(0,64)}`);
                    } else {
                        console.log(`[HostMsg] 收到类型=${typeof data}`);
                    }
                } catch (_) {}
                // 将消息加入队列，异步处理
                queueMessage(key, data);
            } catch (err) {
                console.error(`处理主用户消息失败:`, err);
            }
        });
        
        ws.on('close', () => {
            console.log(`主用户断开连接，KEY: ${key}`);
            
            // 当主用户断开时，从活跃key集合中移除
            activeHostKeys.delete(key);
            console.log(`移除主用户KEY: ${key}，剩余活跃主用户: ${activeHostKeys.size}`);
            
            // 清理该key的消息队列和批处理状态
            messageQueues.delete(key);
            batchProcessingStatus.delete(key);
            
            // 断开所有使用此key的客户端
            const clients = clientsMap.get(key);
            if (clients && clients.size > 0) {
                console.log(`关闭使用此KEY的所有 ${clients.size} 个客户端连接`);
                clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) {
                        try {
                            client.close(1000, '主用户已断开连接');
                        } catch (err) {
                            console.error('关闭客户端连接时出错:', err);
                        }
                    }
                });
                // 清理客户端集合
                clientsMap.delete(key);
            }
        });
    });

    // 處理客戶端連接
    clientServer.on('connection', (ws, req) => {
        try {
            // 獲取客戶端IP
            const clientIP = getClientIP(req);
            console.log(`收到來自 ${clientIP} 的連接請求`);
            
            // 移除封禁机制：不再检查或拒绝被封禁的IP

            const key = getKeyFromUrl(req.url);
            if (!key) {
                console.log(`客戶端 ${clientIP} 連接失敗：缺少key參數（不封禁，只拒絕）`);
                ws.close(1008, '缺少key參數');
                return;
            }
            
            // 安全檢查：驗證key是否是活躍的主用戶key
            if (!activeHostKeys.has(key)) {
                console.log(`客戶端 ${clientIP} 連接拒絕：無效的KEY ${key}（主用戶未連接，不封禁）`);
                ws.close(1008, '無效的KEY，請確保主用戶已連接');
                return;
            }
            
            // 確保這個key有一個客戶端集合
            if (!clientsMap.has(key)) {
                console.log(`為KEY創建新的客戶端集合: ${key}`);
                clientsMap.set(key, new Set());
            }

            const clients = clientsMap.get(key);
            
            // 清理同一個KEY的過期連接
            if (clients && clients.size > 0) {
                const beforeCleanup = clients.size;
                const activeClients = new Set();
                
                // 檢查每個連接是否仍然活躍
                clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) {
                        // 檢查是否是同一IP的連接 (如果需要基於IP清理)
                        const clientReq = client.upgradeReq || client._req || client.request;
                        const existingIP = clientReq ? getClientIP(clientReq) : null;
                        
                        if (existingIP === clientIP) {
                            // 如果發現同IP的活躍連接，關閉它並記錄
                            try {
                                console.log(`發現同IP的活躍連接，關閉舊連接: ${existingIP}, KEY: ${key}`);
                                client.close(1000, '在另一個頁面中建立了新連接');
                            } catch (closeErr) {
                                console.error('關閉舊連接時出錯:', closeErr);
                            }
                        } else {
                            // 不同IP的連接保留
                            activeClients.add(client);
                        }
                    }
                });
                
                // 更新客戶端集合
                if (activeClients.size < beforeCleanup) {
                    console.log(`清理了 ${beforeCleanup - activeClients.size} 個非活躍連接, KEY: ${key}`);
                    clientsMap.set(key, activeClients);
                    // 重新獲取更新後的集合引用
                    const updatedClients = clientsMap.get(key);
                    
                    // 檢查更新後的連接人數限制
                    if (updatedClients.size >= CONFIG.MAX_CLIENTS_PER_KEY) {
                        console.log(`客戶端 ${clientIP} 連接拒絕：KEY ${key} 已達到最大連接數 ${CONFIG.MAX_CLIENTS_PER_KEY}`);
                        ws.close(1008, `連接已達上限(${CONFIG.MAX_CLIENTS_PER_KEY}人)，請稍後再試`);
                        return;
                    }
                    
                    // 將客戶端添加到對應key的集合中
                    updatedClients.add(ws);
                    
                    // 保存客戶端IP，以便後續識別
                    ws._clientIP = clientIP;
                    
                    console.log(`客戶端已添加到集合，當前客戶端數: ${updatedClients.size}/${CONFIG.MAX_CLIENTS_PER_KEY}, KEY: ${key}`);
                } else {
                    // 檢查連接人數限制
                    if (clients.size >= CONFIG.MAX_CLIENTS_PER_KEY) {
                        console.log(`客戶端 ${clientIP} 連接拒絕：KEY ${key} 已達到最大連接數 ${CONFIG.MAX_CLIENTS_PER_KEY}`);
                        ws.close(1008, `連接已達上限(${CONFIG.MAX_CLIENTS_PER_KEY}人)，請稍後再試`);
                        return;
                    }
                    
                    // 將客戶端添加到對應key的集合中
                    clients.add(ws);
                    
                    // 保存客戶端IP，以便後續識別
                    ws._clientIP = clientIP;
                    
                    console.log(`客戶端已添加到集合，當前客戶端數: ${clients.size}/${CONFIG.MAX_CLIENTS_PER_KEY}, KEY: ${key}`);
                }
            } else {
                // 檢查連接人數限制
                if (clients.size >= CONFIG.MAX_CLIENTS_PER_KEY) {
                    console.log(`客戶端 ${clientIP} 連接拒絕：KEY ${key} 已達到最大連接數 ${CONFIG.MAX_CLIENTS_PER_KEY}`);
                    ws.close(1008, `連接已達上限(${CONFIG.MAX_CLIENTS_PER_KEY}人)，請稍後再試`);
                    return;
                }
                
                // 將客戶端添加到對應key的集合中
                clients.add(ws);
                
                // 保存客戶端IP，以便後續識別
                ws._clientIP = clientIP;
                
                console.log(`客戶端已添加到集合，當前客戶端數: ${clients.size}/${CONFIG.MAX_CLIENTS_PER_KEY}, KEY: ${key}`);
            }
            
            console.log(`客戶端 ${clientIP} 連接成功，KEY: ${key} (已驗證)`);
            
            // 發送連接確認消息
            try {
                ws.send(JSON.stringify({
                    type: 'connectionStatus',
                    status: 'connected',
                    message: '客戶端連接成功',
                    currentClients: clients.size,
                    maxClients: CONFIG.MAX_CLIENTS_PER_KEY
                }));
            } catch (sendErr) {
                console.error('發送連接確認消息失敗:', sendErr);
            }
            
            // 監聽客戶端的消息
            ws.on('message', (data) => {
                try {
                    // 嘗試解析消息
                    const message = JSON.parse(data);
                    
                    // 處理ping請求
                    if (message.type === 'ping') {
                        // 立即返回pong響應，不經過隊列處理
                        try {
                            ws.send(JSON.stringify({
                                type: 'pong',
                                timestamp: message.timestamp
                            }));
                        } catch (err) {
                            console.error('發送pong響應失敗:', err);
                        }
                        return; // 不繼續處理其他邏輯
                    }
                    
                    // 處理其他消息類型...
                } catch (err) {
                    console.error(`處理客戶端消息失敗:`, err);
                }
            });
            
            // 當客戶端斷開連接時，從集合中移除
            ws.on('close', () => {
                console.log(`客戶端 ${clientIP} 斷開連接，KEY: ${key}`);
                const clients = clientsMap.get(key);
                if (clients) {
                    clients.delete(ws);
                    console.log(`客戶端已從集合中移除，剩餘客戶端數: ${clients.size}/${CONFIG.MAX_CLIENTS_PER_KEY}`);
                    
                    // 如果沒有客戶端了，清理這個key
                    if (clients.size === 0) {
                        console.log(`沒有剩餘客戶端，移除KEY: ${key}`);
                        clientsMap.delete(key);
                    }
                }
            });
            
            // 處理錯誤
            ws.on('error', (error) => {
                console.error(`客戶端 ${clientIP} WebSocket錯誤，KEY: ${key}`, error);
            });
        } catch (err) {
            console.error('處理客戶端連接時出錯:', err);
        }
    });

    // 从URL中提取key参数
    function getKeyFromUrl(url) {
        if (!url) return null;
        const match = url.match(/[?&]key=([^&]*)/);
        return match ? match[1] : null;
    }

    // 获取客户端IP地址
    function getClientIP(req) {
        let ip = req.headers['x-forwarded-for'] || 
                req.connection.remoteAddress || 
                req.socket.remoteAddress ||
                (req.connection.socket ? req.connection.socket.remoteAddress : null);
        
        // 处理IPv6格式的IP地址
        if (ip && ip.startsWith('::ffff:')) {
            ip = ip.substring(7);
        }
        
        return ip || 'unknown';
    }

    // 已移除封禁相关函数：isIPBanned、banIP

    // 监听端口
    hostHttpServer.listen(CONFIG.HOST_PORT, () => {
        console.log(`主用户WebSocket服务器已启动，监听端口 ${CONFIG.HOST_PORT}`);
    });

    clientHttpServer.listen(CONFIG.CLIENT_PORT, () => {
        console.log(`客户端WebSocket服务器已启动，监听端口 ${CONFIG.CLIENT_PORT}`);
    });
    
    // 定时任务
    setInterval(checkMemoryUsage, CONFIG.MEMORY_CHECK_INTERVAL);
    setInterval(cleanupStaleConnections, CONFIG.CLIENT_CLEANUP_INTERVAL);
    
    // 每小时清理控制台输出
    setInterval(() => {
        try { 
            // 只在内存充足时清理控制台
            const memUsage = process.memoryUsage();
            if (memUsage.heapUsed / memUsage.heapTotal < 0.8) {
                console.clear(); 
            }
        } catch (e) {}
        console.log(`[${new Date().toLocaleTimeString()}] 服务器运行中 - 内存: ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)}MB`);
    }, 60 * 60 * 1000);

    // 定期打印服务器状态（改进版）
    setInterval(() => {
        const activeKeys = Array.from(activeHostKeys);
        const totalClients = Array.from(clientsMap.values())
                               .reduce((total, clients) => total + clients.size, 0);
        const queueSizes = Array.from(messageQueues.entries())
                             .map(([key, queue]) => `${key}:${queue.length}`).join(', ');
        
        const memUsage = process.memoryUsage();
        const memoryInfo = `内存: ${(memUsage.heapUsed / 1024 / 1024).toFixed(2)}MB/${(memUsage.heapTotal / 1024 / 1024).toFixed(2)}MB`;
        
        console.log(`服务器状态: ${activeKeys.length}主用户, ${totalClients}客户端, ${memoryInfo}`);
        if (queueSizes) {
            console.log(`消息队列: ${queueSizes}`);
        }
    }, 60000); // 每分钟打印一次

} else {
    // 工作线程代码：在服务器侧完成坐标转换后再转发
    const { workerId } = workerData;
    console.log(`工作线程 ${workerId} 已启动`);

    // 记录每个key对应的当前地图代码名（例如 Baltic_Main）
    const keyToMapCode = new Map();

    // 定期清理缓存的映射数据
    setInterval(() => {
        // 如果缓存太大，清理最久未使用的
        if (keyToMapCode.size > 1000) {
            const keys = Array.from(keyToMapCode.keys());
            // 保留最近500个，清理其他的
            for (let i = 0; i < keys.length - 500; i++) {
                keyToMapCode.delete(keys[i]);
            }
            console.log(`工作线程${workerId}清理映射缓存: ${keys.length} -> ${keyToMapCode.size}`);
        }
    }, 5 * 60 * 1000); // 每5分钟检查一次

    // 地图代码名 → 显示名
    const mapCodeNameMapping = {
        "PUBG_Escape01_Main": "对峙前线",
        "PUBG_Escape02_Main": "迷雾荒岛",
        "PUBG_Escape04_Main": "冰河禁区",
        "PUBG_Escape05_Main": "烽火荣都",
        "PUBG_Escape06_Main": "实验基地",
        "Baltic_Main": "海岛",
        "PUBG_Neon_Main": "荣都",
        "PUBG_Borderland_Main": "度假岛",
        "PUBG_Savage_Main": "雨林",
        "PUBG_Desert02_Main": "沙漠",
        "PUBG_Dam_Main": "零号大坝",
        "PUBG_Valley_Main": "长弓溪谷",
        "Split": "霓虹町",
        "Haven": "隐世修所",
        "Ascent": "亚海悬城",
        "Fracture": "裂变峡谷",
        "Sunset": "日落之城",
        "Lotus": "莲华古城",
        "Icebox": "森寒冬港",
        "Pearl": "深海明珠",
        "Breeze": "微风岛屿",
        "Corrode": "盐海矿镇",
        "Abyss": "幽邃地窟",
        "Bind": "源工重镇"
    };

    function convertMapCodeToName(mapCode) {
        return mapCodeNameMapping[mapCode] || mapCode || "";
    }

    // 参考点（与前端保持一致）
    const mapReferencePoints = {
        "烽火荣都": { p1: { world: {x: 453623, y: 676092}, map: {x: 1570.33, y: 1414.67} }, p2: { world: {x: 464926, y: 701742}, map: {x: 1748.64, y: 1815.45} }, p3: { world: {x: 458915, y: 730147}, map: {x: 1655.20, y: 2267.96} }, worldSize: 2600 },
        "迷雾荒岛": { p1: { world: {x: 95487, y: 110750}, map: {x: 2837.71, y: 2720.80} }, p2: { world: {x: 44825.8, y: 77211}, map: {x: 1452.80, y: 1810.77} }, p3: { world: {x: 53490.1, y: 51297.8}, map: {x: 1691.87, y: 1101.59} }, worldSize: 1500 },
        "冰河禁区": { p1: { world: {x: 210818, y: 87123.9}, map: {x: 3266.61, y: 686.23} }, p2: { world: {x: 152440, y: 105124}, map: {x: 2070.15, y: 1054.70} }, p3: { world: {x: 80452.2, y: 169955}, map: {x: 596.36, y: 2383.04} }, worldSize: 2000 },
        "实验基地": { p1: { world: {x: 29416.3, y: 37577.5}, map: {x: 555, y: 884} }, p2: { world: {x: 71776.1, y: 38423.6}, map: {x: 3034, y: 937} }, p3: { world: {x: 66886.8, y: 66603.7}, map: {x: 2748, y: 2585} }, worldSize: 700 },
        "海岛": { p1: { world: {x: 197491, y: 447953}, map: {x: 1003.86, y: 2274.96} }, p2: { world: {x: 448192, y: 412801}, map: {x: 2276.72, y: 2096.70} }, p3: { world: {x: 515679, y: 246280}, map: {x: 2618.80, y: 1250.85} }, worldSize: 8000 },
        "沙漠": { p1: { world: {x: 197491, y: 447953}, map: {x: 1003.86, y: 2274.96} }, p2: { world: {x: 448192, y: 412801}, map: {x: 2276.72, y: 2096.70} }, p3: { world: {x: 515679, y: 246280}, map: {x: 2618.80, y: 1250.85} }, worldSize: 8000 },
        "荣都": { p1: { world: {x: 197491, y: 447953}, map: {x: 1003.86, y: 2274.96} }, p2: { world: {x: 448192, y: 412801}, map: {x: 2276.72, y: 2096.70} }, p3: { world: {x: 515679, y: 246280}, map: {x: 2618.80, y: 1250.85} }, worldSize: 8000 },
        "雨林": { p1: { world: {x: 365122, y: 147823}, map: {x: 3719.31, y: 1497.91} }, p2: { world: {x: 283167, y: 197113}, map: {x: 2879.89, y: 1993.92} }, p3: { world: {x: 315155, y: 289110}, map: {x: 3205.24, y: 2936.78} }, worldSize: 4000 },
        "度假岛": { p1: { world: {x: 67469.2, y: 64514.7}, map: {x: 1827.52, y: 1746.53} }, p2: { world: {x: 85006.1, y: 140726}, map: {x: 2302.26, y: 3811.96} }, p3: { world: {x: 98537.2, y: 94422.3}, map: {x: 2671.23, y: 2557.12} }, worldSize: 1200 },
        "对峙前线": { p1: { world: {x: 524360, y: 235278}, map: {x: 1665.97, y: 1863.91} }, p2: { world: {x: 548989, y: 267007}, map: {x: 2227.16, y: 2586.70} }, p3: { world: {x: 573627, y: 213621}, map: {x: 1589.84, y: 1368.53} }, worldSize: 1800 },
        "零号大坝": { p1: { world: {x: 27144.7, y: 30467.4}, map: {x: 2712, y: 2320} }, p2: { world: {x: -3687.13, y: -26732.3}, map: {x: 1932, y: 875} }, p3: { world: {x: -36113, y: -882.163}, map: {x: 1116, y: 1527} }, worldSize: 8000 },
        "长弓溪谷": { p1: { world: { x: -32057.320312, y: 58715.484375 }, map: { x: 1015, y: 2723 } }, p2: { world: { x: -35594.531250, y: -39949.425781 }, map: { x: 2798, y: 2655 } }, p3: { world: { x: -94630.484375, y: 9738.929688 }, map: {x: 1908, y: 1593} }, worldSize: 8000 },
        "航天基地": { p1: { world: {x: 17800.41, y:-28353.59}, map: {x: 2591, y: 962} }, p2: { world: {x:-26581.21 , y: -5895.21}, map: {x: 1179, y: 1669} }, p3: { world: {x:376.22, y:16126.04}, map: {x:2044 , y: 2357} }, worldSize: 8000 },
        "巴克什": { p1: { world: {x: -20720.13, y: -31612.95}, map: {x: 1299, y: 1215} }, p2: { world: {x: -498.01, y:  -21745.12}, map: {x: 1986, y: 1550} }, p3: { world: {x:-8618.13 , y:12940.22 }, map: {x:1709 , y: 2735} }, p4: { world: {x: -4665.38, y: 7324.85}, map: {x: 1848, y: 2548} }, worldSize: 8000 },
        "潮汐监狱": { p1: { world: {x: 7876.06, y: -8306.98}, map: {x: 2655, y: 1518} }, p2: { world: {x:11529.8, y: -11191.7}, map: {x: 2854, y:1374} }, p3: { world: {x: -14323.1, y: -3212.52}, map: {x: 1400, y: 1823} },p4: { world: {x: 11744.546875, y: 553.828125}, map: {x: 2867, y: 2030} }, worldSize: 8000 },
        "隐世修所": { p1: { world: {x: 2792.469482, y: -3152.226318}, map: {x: 3507, y: 1747} }, p2: { world: {x: 7356.251953, y: -8442.872070}, map: {x: 1883, y: 364} }, p3: { world: {x: 3290.329590, y: -14085.155273}, map: {x: 160, y: 1610} }, worldSize: 1024 },
        "霓虹町": { p1: { world: {x: 1742.227173, y: -10007.988281}, map: {x: 230, y: 2300} }, p2: { world: {x: -3447.132568, y: -7807.999512}, map: {x: 947, y: 3970} }, p3: { world: {x: 2732.964355, y: 929.726868}, map: {x: 3775, y: 1973} }, worldSize: 1024 },
        "亚海悬城": { p1: { world: {x: 1042.007935, y: -11057.979492}, map: {x: 180, y: 2045} }, p2: { world: {x: -4837.990234, y: -7092.034668}, map: {x: 1311, y: 3733} }, p3: { world: {x: 3857.989746, y: -42.051777}, map: {x: 3330, y: 1241} }, worldSize: 1024 },
        "裂变峡谷": { p1: { world: {x: 9108.038086, y: 4651.941895}, map: {x: 3770, y: 1818} }, p2: { world: {x: 7392.101074, y: -6601.983398}, map: {x: 168, y: 2375} }, p3: { world: {x: 3012.951172, y: -1397.186401}, map: {x: 1833, y: 3780} }, worldSize: 1024 },
        "日落之城": { p1: { world: {x: 6132.919434, y: 1957.947754}, map: {x: 2683, y: 140} }, p2: { world: {x: 1042.019165, y: 4757.921875}, map: {x: 3578, y: 1784} }, p3: { world: {x: -5927.676758, y: 1297.737793}, map: {x: 2490, y: 4030} }, worldSize: 1024 },
        "莲华古城": { p1: { world: {x: 10961.391602, y: 3558.223877}, map: {x: 2929, y: 500} }, p2: { world: {x: 6454.826172, y: -5455.321289}, map: {x: 239, y: 1838} }, p3: { world: {x: 408.001923, y: 43.930313}, map: {x: 1874, y: 3657} }, worldSize: 1024 },
        "森寒冬港": { p1: { world: {x: -2449.184326, y: 6102.990723}, map: {x: 3695, y: 1974} }, p2: { world: {x: -8257.975586, y: 4357.989258}, map: {x: 3185, y: 3684} }, p3: { world: {x: -5282.979980, y:-3732.970947}, map: {x: 761, y: 2815} }, worldSize: 1024 },
        "深海明珠": { p1: { world: {x: 8546.731445, y:  929.191589}, map: {x: 2265, y: 1015} }, p2: { world: {x: 7352.973633, y: -5557.902344}, map: {x: 186, y: 1394} }, p3: { world: {x:-480.922058, y: 1857.995361}, map: {x: 2563, y: 3923} }, worldSize: 1024 },
        "微风岛屿": { p1: { world: {x: 11390.308594, y: 2507.999268}, map: {x: 2624, y: 140} }, p2: { world: {x:6334.297852, y: -6157.984863}, map: {x: 144, y: 1591} }, p3: { world: {x: -1497.743652, y: 716.322205}, map: {x: 2093, y: 3841} }, worldSize: 1024 },
        "盐海矿镇": { p1: { world: {x: 319.978119, y: -7018.930176}, map: {x: 145, y: 1950} }, p2: { world: {x: -2442.041748, y: -842.100342}, map: {x: 1920, y: 2739} }, p3: { world: {x:-631.284485, y: 6466.7041002}, map: {x: 4020, y: 2222} }, worldSize: 1024 },
        "幽邃地窟": { p1: { world: {x: -557.001831, y: -5728.285156}, map: {x: 136, y: 2236} }, p2: { world: {x: 5932.999023, y: -2151.966309}, map: {x: 1328, y: 82} }, p3: { world: {x: 1556.843750, y: 5733.024414}, map: {x: 3956, y: 1525} }, worldSize: 1024 },
        "源工重镇": { p1: { world: {x: 16043.631836, y: -1331.807373}, map: {x: 2021, y: 64} }, p2: { world: {x: 13157.941406, y: -6257.989258}, map: {x: 845, y: 773} }, p3: { world: {x: 5641.988770, y: 4957.899902}, map: {x: 3568, y: 2593} }, worldSize: 1024 }
    };

    function calculateTransform(mapDisplayName) {
        const ref = mapReferencePoints[mapDisplayName];
        if (!ref) {
            // 默认单位缩放，避免崩溃
            return { a: 0.01, b: 0, c: 0, d: 0, e: 0.01, f: 0 };
        }
        
        // 检查是否有第四个坐标点
        const hasP4 = ref.p4 !== undefined;
        
        if (hasP4) {
            // 四个坐标点，使用最小二乘法
            const points = [ref.p1, ref.p2, ref.p3, ref.p4];
            const X = points.map(p => [p.world.x, p.world.y, 1]);
            const Yx = points.map(p => p.map.x);
            const Yy = points.map(p => p.map.y);
            
            // 计算 XtX (3x3)
            const XtX = [[0,0,0],[0,0,0],[0,0,0]];
            for (let i=0;i<3;i++) for (let j=0;j<3;j++) for (let k=0;k<4;k++) XtX[i][j]+= X[k][i]*X[k][j];
            
            const det = XtX[0][0] * (XtX[1][1] * XtX[2][2] - XtX[1][2] * XtX[2][1])
                      - XtX[0][1] * (XtX[1][0] * XtX[2][2] - XtX[1][2] * XtX[2][0])
                      + XtX[0][2] * (XtX[1][0] * XtX[2][1] - XtX[1][1] * XtX[2][0]);
            
            if (Math.abs(det) < 1e-10) {
                return { a: 0.01, b: 0, c: 0, d: 0, e: 0.01, f: 0 };
            }
            
            const invXtX = [[0,0,0],[0,0,0],[0,0,0]];
            invXtX[0][0] = (XtX[1][1] * XtX[2][2] - XtX[1][2] * XtX[2][1]) / det;
            invXtX[0][1] = (XtX[0][2] * XtX[2][1] - XtX[0][1] * XtX[2][2]) / det;
            invXtX[0][2] = (XtX[0][1] * XtX[1][2] - XtX[0][2] * XtX[1][1]) / det;
            invXtX[1][0] = (XtX[1][2] * XtX[2][0] - XtX[1][0] * XtX[2][2]) / det;
            invXtX[1][1] = (XtX[0][0] * XtX[2][2] - XtX[0][2] * XtX[2][0]) / det;
            invXtX[1][2] = (XtX[0][2] * XtX[1][0] - XtX[0][0] * XtX[1][2]) / det;
            invXtX[2][0] = (XtX[1][0] * XtX[2][1] - XtX[1][1] * XtX[2][0]) / det;
            invXtX[2][1] = (XtX[0][1] * XtX[2][0] - XtX[0][0] * XtX[2][1]) / det;
            invXtX[2][2] = (XtX[0][0] * XtX[1][1] - XtX[0][1] * XtX[1][0]) / det;
            
            const XtYx = [0,0,0], XtYy = [0,0,0];
            for (let i=0;i<3;i++) for (let k=0;k<4;k++){ XtYx[i]+= X[k][i]*Yx[k]; XtYy[i]+= X[k][i]*Yy[k]; }
            
            const betaX = [0,0,0], betaY=[0,0,0];
            for (let i=0;i<3;i++) for (let j=0;j<3;j++){ betaX[i]+= invXtX[i][j]*XtYx[j]; betaY[i]+= invXtX[i][j]*XtYy[j]; }
            
            return { a: betaX[0], b: betaX[1], c: betaX[2], d: betaY[0], e: betaY[1], f: betaY[2] };
        } else {
            // 三个坐标点，使用原来的精确计算方法
            const p1 = ref.p1, p2 = ref.p2, p3 = ref.p3;
            const X = [
                [p1.world.x, p1.world.y, 1],
                [p2.world.x, p2.world.y, 1],
                [p3.world.x, p3.world.y, 1]
            ];
            const Yx = [p1.map.x, p2.map.x, p3.map.x];
            const Yy = [p1.map.y, p2.map.y, p3.map.y];
            
            // 计算 XtX
            const XtX = [[0,0,0],[0,0,0],[0,0,0]];
            for (let i=0;i<3;i++) for (let j=0;j<3;j++) for (let k=0;k<3;k++) XtX[i][j]+= X[k][i]*X[k][j];
            
            const det = XtX[0][0] * (XtX[1][1] * XtX[2][2] - XtX[1][2] * XtX[2][1])
                      - XtX[0][1] * (XtX[1][0] * XtX[2][2] - XtX[1][2] * XtX[2][0])
                      + XtX[0][2] * (XtX[1][0] * XtX[2][1] - XtX[1][1] * XtX[2][0]);
            
            if (Math.abs(det) < 1e-10) {
                return { a: 0.01, b: 0, c: 0, d: 0, e: 0.01, f: 0 };
            }
            
            const invXtX = [[0,0,0],[0,0,0],[0,0,0]];
            invXtX[0][0] = (XtX[1][1] * XtX[2][2] - XtX[1][2] * XtX[2][1]) / det;
            invXtX[0][1] = (XtX[0][2] * XtX[2][1] - XtX[0][1] * XtX[2][2]) / det;
            invXtX[0][2] = (XtX[0][1] * XtX[1][2] - XtX[0][2] * XtX[1][1]) / det;
            invXtX[1][0] = (XtX[1][2] * XtX[2][0] - XtX[1][0] * XtX[2][2]) / det;
            invXtX[1][1] = (XtX[0][0] * XtX[2][2] - XtX[0][2] * XtX[2][0]) / det;
            invXtX[1][2] = (XtX[0][2] * XtX[1][0] - XtX[0][0] * XtX[1][2]) / det;
            invXtX[2][0] = (XtX[1][0] * XtX[2][1] - XtX[1][1] * XtX[2][0]) / det;
            invXtX[2][1] = (XtX[0][1] * XtX[2][0] - XtX[0][0] * XtX[2][1]) / det;
            invXtX[2][2] = (XtX[0][0] * XtX[1][1] - XtX[0][1] * XtX[1][0]) / det;
            
            const XtYx = [0,0,0], XtYy = [0,0,0];
            for (let i=0;i<3;i++) for (let k=0;k<3;k++){ XtYx[i]+= X[k][i]*Yx[k]; XtYy[i]+= X[k][i]*Yy[k]; }
            
            const betaX = [0,0,0], betaY=[0,0,0];
            for (let i=0;i<3;i++) for (let j=0;j<3;j++){ betaX[i]+= invXtX[i][j]*XtYx[j]; betaY[i]+= invXtX[i][j]*XtYy[j]; }
            
            return { a: betaX[0], b: betaX[1], c: betaX[2], d: betaY[0], e: betaY[1], f: betaY[2] };
        }
    }

    function affineTransform(x, y, m) {
        const result = { x: m.a * x + m.b * y + m.c, y: m.d * x + m.e * y + m.f };
       // try { console.log(`[DEBUG] 坐标转换: 世界(${x}, ${y}) -> 地图(${result.x}, ${result.y})`); } catch (_) {}
        return result;
    }

    function parseCustomPlayersFormat(data) {
        try {
            if (!data || typeof data !== 'string' || !data.startsWith('players{')) return [];
            let content;
            const firstBrace = data.indexOf('{');
            const lastBrace = data.lastIndexOf('}');
            if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
                content = data.substring(firstBrace + 1, lastBrace);
            } else {
                // 兼容缺少结尾 '}' 的情况，尽力解析
                try { console.log(`[Worker ${workerId}] players 消息缺少结束符 '}}'，将尝试宽松解析`); } catch (_) {}
                content = data.substring(8);
            }
            const isOldFormat = content.includes('ID=') || content.includes('id=');
            const players = [];
            if (isOldFormat) {
                let current = {};
                const pairs = content.split(',');
                for (const pair of pairs) {
                    if (!pair || !pair.includes('=')) continue;
                    const [key, value] = pair.split('=');
                    if (key === 'ID' || key === 'id') {
                        if (Object.keys(current).length > 0) players.push(current);
                        current = { id: parseInt(value) };
                    } else if (key === 'x' || key === 'y' || key === 'hp' || key === 'type') {
                        current[key] = value === '' ? '' : parseFloat(value);
                    } else if (key === 'signal' || key === 'team') {
                        current[key] = value === '' ? '' : parseInt(value);
                    } else {
                        current[key] = value;
                    }
                }
                if (Object.keys(current).length > 0) players.push(current);
                return players;
            } else {
                const values = content.split(',');
                const fieldNames = ['id', 'name', 'type', 'x', 'y', 'hp', 'team', 'hold', 'signal', 'status', 'z', 'dirX', 'dirY', 'distance', 'role'];
                let fieldIndex = 0;
                let current = {};
                for (let i=0;i<values.length;i++) {
                    const value = values[i];
                    const fieldName = fieldNames[fieldIndex];
                    if (fieldIndex === 0 && value !== '') {
                        if (Object.keys(current).length > 0) players.push(current);
                        current = {};
                    }
                    if (['id','type','team','signal'].includes(fieldName)) {
                        current[fieldName] = value === '' ? '' : parseInt(value);
                    } else if (['x','y','hp','z','dirX','dirY','distance'].includes(fieldName)) {
                        current[fieldName] = value === '' ? '' : parseFloat(value);
                    } else {
                        current[fieldName] = value;
                    }
                    fieldIndex = (fieldIndex + 1) % fieldNames.length;
                }
                if (Object.keys(current).length > 0) players.push(current);
                return players;
            }
        } catch (e) {
            return [];
        }
        }

    // 尝试把形如 "{109,97,...}" 或 "[109,97,...]" 的数字列表转成 UTF-8 文本
    function normalizeTextIfNumericList(text) {
        try {
            const trimmed = String(text).trim();
            // 仅包含数字/逗号/空白/括号时尝试解析
            if (/^[\s\[{\(]*\d+(\s*,\s*\d+)*[\s\]\}\)]*$/.test(trimmed)) {
                const digits = trimmed.replace(/[\s\[\]\{\}\(\)]/g, '').split(',');
                const bytes = [];
                for (const d of digits) {
                    const n = Number(d);
                    if (!Number.isFinite(n) || n < 0 || n > 255) return text; // 非字节范围，放弃
                    bytes.push(n & 0xFF);
                }
                const buf = Buffer.from(bytes);
                const str = buf.toString('utf8');
                try { console.log(`[Worker ${workerId}] 解析数字列表为文本:`, str); } catch (_) {}
                return str;
            }
            return text;
        } catch (_) {
            return text;
        }
    }

    // 监听来自主线程的消息
    parentPort.on('message', ({ key, batch, batchId }) => {
        try {
            batch.forEach(message => {
                try {
                    let data = message;
                    // 将Buffer/Uint8Array/ArrayBuffer转为字符串；非字符串保留
                    try {
                        if (Buffer.isBuffer(data)) {
                            data = data.toString('utf8');
                        } else if (data && data.buffer && data.byteLength !== undefined) {
                            // 处理 Uint8Array 等 typed array
                            data = Buffer.from(data.buffer, data.byteOffset || 0, data.byteLength).toString('utf8');
                        } else if (data instanceof ArrayBuffer) {
                            data = Buffer.from(data).toString('utf8');
                        }
                    } catch (_) {}

                    // 兼容 {109,97,...} 或 [109,97,...] 这类数字序列
                    if (typeof data === 'string') {
                        const before = data;
                        data = normalizeTextIfNumericList(data);
                        if (data !== before) {
                            try { console.log(`[Worker ${workerId}] 数字序列 -> 文本:`, data); } catch (_) {}
                        }
                    }

                    if (typeof data === 'string') {
                        // 处理地图名更新
                        if (data.startsWith('mapName=')) {
                           // try { console.log(`[Worker ${workerId}] 命中 mapName 前缀，原文:`, data); } catch (_) {}
                            const mapCode = data.substring(8).trim();
                            if (mapCode) {
                                keyToMapCode.set(key, mapCode);
                                try {
                                    const displayName = convertMapCodeToName(mapCode);
                                    // console.log(`[Worker ${workerId}] 接收到 mapName: key=${key}, mapCode=${mapCode}, displayName=${displayName}`);
                                } catch (_) {}
                            } else {
                                // console.log(`[Worker ${workerId}] 收到 mapName= 但解析为空，原始:`, data);
                            }
                            // 原样转发该字符串（客户端已支持）
                            parentPort.postMessage({ key, data, batchId });
                            return;
                        }

                        // 处理players压缩格式（明文）并转换坐标
                        if (data.startsWith('players{')) {
                            // try { console.log(`[Worker ${workerId}] 收到 players 文本，len=${data.length}`); } catch (_) {}
                            const players = parseCustomPlayersFormat(data);
                            const mapCode = keyToMapCode.get(key);
                            if (!mapCode) {
                               // try { console.log(`[Worker ${workerId}] 尚未记录 mapName，原样转发 players`); } catch (_) {}
                                parentPort.postMessage({ key, data, batchId });
                                return;
                            }
                            if (players.length === 0) {
                                // try { console.log(`[Worker ${workerId}] players 解析为空，原样转发`); } catch (_) {}
                                parentPort.postMessage({ key, data, batchId });
                                return;
                            }
                            const displayName = convertMapCodeToName(mapCode);
                            const M = calculateTransform(displayName);
                            const samples = [];
                            const converted = players.map(p => {
                                if (typeof p.x === 'number' && typeof p.y === 'number') {
                                    const pt = affineTransform(p.x, p.y, M);
                                    if (samples.length < 5) {
                                        samples.push({ id: p.id, from: { x: p.x, y: p.y }, to: { x: pt.x, y: pt.y } });
                                    }
                                    // 在第14个参数中填入世界坐标信息，用于客户端距离计算
                                    const worldCoords = `${p.x}-${p.y}`;
                                    return { ...p, x: pt.x, y: pt.y, worldCoords: worldCoords };
                                }
                                return p;
                            });
                            try {
                               // console.log(`[Worker ${workerId}] 坐标转换完成: key=${key}, map=${displayName}, 共 ${converted.length} 个，示例:`, samples);
                            } catch (_) {}
                            const out = JSON.stringify({ mapName: mapCode, coords: 'map', players: converted });
                            // 让主线程直接发送字符串，已启用 perMessageDeflate 会自动压缩
                            parentPort.postMessage({ key, data: out, batchId });
                            return;
                        }
                    }

                    // 其他数据类型原样转发
                    parentPort.postMessage({ key, data: message, batchId });
                } catch (innerErr) {
                    // 出现错误则原样转发
                    parentPort.postMessage({ key, data: message, batchId });
                }
            });
        } catch (err) {
            console.error(`工作线程 ${workerId} 处理批次 ${batchId} 出错:`, err);
        }
    });
}