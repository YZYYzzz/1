document.addEventListener('DOMContentLoaded', function() {
    // 获取元素
    const sizeBtns = document.querySelectorAll('.size-btn');
    const roleButtons = document.querySelectorAll('.role-btn');
    const checkboxes = document.querySelectorAll('input[type="checkbox"]');
    const zoomInBtn = document.getElementById('zoom-in');
    const zoomOutBtn = document.getElementById('zoom-out');
    const zoomLevel = document.getElementById('zoom-level');
    const refreshBtn = document.getElementById('refresh-btn');
    
    // 大小按钮点击事件
    sizeBtns.forEach(btn => {
        btn.addEventListener('click', function() {
            // 移除所有按钮的选中状态
            sizeBtns.forEach(b => b.style.backgroundColor = '#f0f0f0');
            // 设置当前按钮为选中状态
            this.style.backgroundColor = '#bbbbbb';
        });
    });
    
    // 角色按钮点击事件
    roleButtons.forEach(btn => {
        btn.addEventListener('click', function() {
            // 移除其他按钮的选中状态
            roleButtons.forEach(b => b.style.border = 'none');
            // 设置当前按钮为选中状态
            this.style.border = '2px solid #333';
        });
    });
    
    // 复选框变化事件
    checkboxes.forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            console.log(`${this.id} is now ${this.checked ? 'checked' : 'unchecked'}`);
        });
    });
    
    // 缩放功能
    let currentZoom = 100;
    zoomInBtn.addEventListener('click', function() {
        if (currentZoom < 5000) {
            currentZoom += 10;
            zoomLevel.textContent = `${currentZoom}%`;
        }
    });
    
    zoomOutBtn.addEventListener('click', function() {
        if (currentZoom > 50) {
            currentZoom -= 10;
            zoomLevel.textContent = `${currentZoom}%`;
        }
    });
    
    // 刷新按钮
    refreshBtn.addEventListener('click', function() {
        location.reload();
    });
    
    // 模拟URL参数处理
    function getParameterByName(name) {
        const url = window.location.href;
        name = name.replace(/[\[\]]/g, '\\$&');
        const regex = new RegExp('[?&]' + name + '(=([^&#]*)|&|#|$)');
        const results = regex.exec(url);
        if (!results) return null;
        if (!results[2]) return '';
        return decodeURIComponent(results[2].replace(/\+/g, ' '));
    }
    
    // 检查URL中是否有key参数
    const keyParam = getParameterByName('key');
    if (!keyParam) {
        // 如果没有key参数，可以添加提示或重定向
        console.log('缺少必要的key参数');
    } else {
        console.log(`当前key值: ${keyParam}`);
        // 这里可以添加根据key加载不同内容的逻辑
    }
}); 