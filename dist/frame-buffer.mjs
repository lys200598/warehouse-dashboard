// Playback buffer for live frames. The server keeps the requested speed on
// average, but single frames can arrive late (synchronous save, dispatch round).
// Holding frames for a short delay and releasing them at simulation pace hides
// that jitter. Frames that are not "running" (pause, step, ready, complete,
// failed) and run changes are released at once, so controls never lag.
export const DEFAULT_BUFFER_MS=3000;

export function bufferDelayFromQuery(search=''){
  const params=new URLSearchParams(search);
  if(!params.has('buffer'))return DEFAULT_BUFFER_MS;
  const value=Number(params.get('buffer'));
  return Number.isFinite(value)?Math.max(0,Math.min(10000,value)):DEFAULT_BUFFER_MS;
}

export function createFrameBuffer({delayMs=DEFAULT_BUFFER_MS,maxFrames=40}={}){
  let queue=[],anchor=null;
  const identity=f=>`${f.runId}|${f.runtimeInstanceId}`;
  const drain=()=>{const out=queue.map(item=>item.frame);queue=[];return out;};
  return {
    push(frame,now){
      const f=frame?.snapshot||frame;
      if(!f||delayMs<=0||f.status!=='running'||!Number.isFinite(f.elapsed)){
        anchor=null;return [...drain(),frame];
      }
      const speed=f.speed>0?f.speed:1;
      let released=[];
      if(!anchor||anchor.id!==identity(f)||anchor.speed!==speed||f.elapsed<anchor.sim){
        released=drain();anchor={id:identity(f),sim:f.elapsed,wall:now+delayMs,speed};
      }
      let due=anchor.wall+(f.elapsed-anchor.sim)*1000/speed;
      if(due<now){
        // The server fell behind by more than the buffer: refill half of it once.
        anchor={id:identity(f),sim:f.elapsed,wall:now+delayMs/2,speed};due=anchor.wall;
      }
      queue.push({frame,due});
      while(queue.length>maxFrames)released.push(queue.shift().frame);
      return released;
    },
    due(now){const out=[];while(queue.length&&queue[0].due<=now)out.push(queue.shift().frame);return out;},
    flush(){anchor=null;return drain();},
    size(){return queue.length;},
  };
}
