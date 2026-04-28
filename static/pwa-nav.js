(function(){
  if(window.navigator.standalone !== true) return;
  document.addEventListener('click', function(e){
    var node = e.target;
    while(node && node.nodeName !== 'A'){
      node = node.parentNode;
    }
    if(node && node.nodeName === 'A' && node.href &&
       node.hostname === location.hostname && !node.target &&
       node.href.indexOf('javascript:') !== 0){
      e.preventDefault();
      e.stopPropagation();
      location.href = node.href;
    }
  }, true);
})();
