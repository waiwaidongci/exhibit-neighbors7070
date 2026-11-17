import {research} from '../stores/research.js';import {session} from '../stores/session.js';import {withdrawalsApi} from '../api/research.js';import {withdrawalStates} from '../constants/enums.js';import {esc} from '../utils/format.js';import {action,toast} from '../hooks/actions.js';import {table} from '../components/common/table.js';
const badge=w=>'<span class="status '+esc(w.state)+'">'+esc(withdrawalStates[w.state]||w.state)+'</span>';
export function withdrawalPage(){
  const rows=research.withdrawals.map(w=>({
    credential:w.credential+'…',state:withdrawalStates[w.state]||w.state,
    removed_ratings:w.removed_ratings,
    affected:w.affected_runs.length+' 项研究 / '+w.affected_notes.length+' 份备选',
    artifacts:w.artifact_purged+' / '+w.artifact_total,
    residual:w.residual_findings&&w.residual_findings.length?w.residual_findings.length+' 项待清除':'核验通过'
  }));
  const pending=research.withdrawals.filter(w=>w.state!=='completed');
  return '<section class="page-heading"><p class="eyebrow">07 / CONSENT & ERASURE</p><h1>访客撤回试展授权，数据即被阻断并永久清除。</h1><p>受理瞬间阻断新评分与新计算；跨批次清除评分、冻结输入与推荐邻居，受影响研究及已批准备选停止取用并删除留档。仅残留核验通过后才报完成。</p></section>'
    +(session.user.role==='admin'?'<section class="entry-sheet"><h2>发起授权撤回</h2><form id="withdrawal-form"><label>匿名访客标识（visitor）<input name="visitor" required maxlength="600" placeholder="如 V007"></label><p class="fine">系统仅保存该标识的单向匿名凭据（HMAC），审计与列表不保留原始标识。</p><button type="submit">受理撤回并开始清除 →</button></form></section>':'<section class="entry-sheet"><h2>阅读权限</h2><p>授权撤回由负责人受理；当前账号可查看撤回进度与阻断状态。</p></section>')
    +'<section><h2>撤回处理台账</h2>'+(rows.length?table(rows,[{key:'credential',label:'匿名凭据'},{key:'state',label:'状态'},{key:'removed_ratings',label:'已清除评分'},{key:'affected',label:'受影响范围'},{key:'artifacts',label:'留档文件清除'},{key:'residual',label:'残留核验'}],{limit:50}):'<div class="empty">暂无撤回请求。</div>')+'</section>'
    +(pending.length?'<section class="pending"><span class="spinner"></span><h2>存在 '+pending.length+' 个清理中的撤回单</h2><p>页面每 2 秒核验进度；只有残留核验通过才会显示“已完成”。若中断，可携带访客标识重试，重试不重复清理、不误删他人数据。</p>'
      +(session.user.role==='admin'?'<form id="retry-form" style="margin-top:12px"><label>中断续清（可选，输入该访客标识）<input name="visitor" maxlength="600" placeholder="留空则尝试自动还原"></label><button type="submit" class="secondary">重试清理 →</button></form>':'')+'</section>':'')
    +(research.withdrawals.some(w=>w.state==='completed')?'<section class="method-sheet"><h2>完成口径</h2><p>已完成撤回单的评分、冻结输入、推荐邻居与留档文件均通过残留核验；旧任务重放与重复导入均无法复活该访客，其他访客评分与无关研究保持不变。</p></section>':'');
}
export function bindWithdrawal(refresh){
  if(session.user.role==='admin'){
    action('#withdrawal-form',async form=>{
      const data=Object.fromEntries(new FormData(form));
      const w=await withdrawalsApi.create({visitor:data.visitor});
      form.reset();
      toast(w.state==='completed'?'撤回完成，残留核验通过':'撤回已受理，正在清理…',w.state!=='completed');
      await refresh();
    });
    action('#retry-form',async form=>{
      const data=Object.fromEntries(new FormData(form));
      const target=research.withdrawals.find(w=>w.state!=='completed');
      if(!target)return;
      const w=await withdrawalsApi.retry(target.id,{visitor:data.visitor||''});
      toast(w.state==='completed'?'续清完成，残留核验通过':'仍有残留，已保持阻断状态',w.state!=='completed');
      form.reset();await refresh();
    });
  }
}
