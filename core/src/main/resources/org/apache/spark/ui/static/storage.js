var DB;
try {
  var x = 'test_localstorage_available_' + Date.now();
  localStorage.setItem(x, x);
  var y = localStorage.getItem(x);
  localStorage.removeItem(x);
  if (x !== y) {throw new Error();}
  DB = localStorage;
} catch (exception) {
  DB = new MemoryStorage('spark');
}