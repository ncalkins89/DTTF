var dagcomponentfuncs = window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {};

dagcomponentfuncs.TeamCommitmentBar = function(props) {
    var R     = window.React;
    var used  = props.data.team_picks_used || 0;
    var total = props.data.team_total      || 1;
    var pct   = Math.min(used / total, 1) * 100;
    var color = props.data.team_color      || '#0071e3';
    var dim   = props.data.Display_Status &&
                (props.data.Display_Status[0] === '✓' || props.data.Display_Status[0] === '❌');

    return R.createElement('div', {
        style: {display:'flex', alignItems:'center', gap:'5px', width:'100%', opacity: dim ? 0.4 : 1}
    },
        R.createElement('div', {
            style: {flex:1, height:'5px', background:'#e5e5ea', borderRadius:'3px', overflow:'hidden', minWidth:'30px'}
        },
            R.createElement('div', {
                style: {width: pct + '%', height:'100%', background: color, borderRadius:'3px'}
            })
        ),
        R.createElement('span', {
            style: {fontSize:'10px', color:'#8e8e93', whiteSpace:'nowrap', fontVariantNumeric:'tabular-nums'}
        }, used + '/' + total)
    );
};
