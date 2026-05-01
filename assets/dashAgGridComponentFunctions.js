var dagcomponentfuncs = window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {};

dagcomponentfuncs.TeamCommitmentBar = function(props) {
    var used  = props.data.team_picks_used || 0;   // remaining eligible players
    var total = props.data.team_total      || 1;
    var pct   = Math.min(used / total, 1) * 100;
    var dim   = props.data.Display_Status &&
                (props.data.Display_Status[0] === '✓' || props.data.Display_Status[0] === '❌');

    var barColor = used === 0 ? '#dc2626'       // empty → red
                 : used === total ? '#16a34a'   // full → green
                 : '#aaa';                      // partial → gray

    var numColor = used === 0 ? '#dc2626'
                 : used === total ? '#16a34a'
                 : '#8e8e93';

    return React.createElement('div', {
        style: {display:'flex', alignItems:'center', gap:'6px', width:'100%', padding:'0 4px', opacity: dim ? 0.4 : 1}
    },
        React.createElement('div', {
            style: {position:'relative', flex:'1 1 0', minWidth:'20px', height:'8px',
                    background:'#e5e5ea', borderRadius:'4px', overflow:'hidden'}
        },
            React.createElement('div', {
                style: {position:'absolute', left:0, top:0, bottom:0, width: pct + '%',
                        background: barColor, borderRadius:'4px'}
            })
        ),
        React.createElement('span', {
            style: {fontSize:'10px', color: numColor, whiteSpace:'nowrap', flexShrink:0,
                    fontVariantNumeric:'tabular-nums', fontWeight: used === 0 ? '700' : '400'}
        }, used + '/' + total)
    );
};
