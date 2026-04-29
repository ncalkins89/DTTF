var dagcomponentfuncs = window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {};

dagcomponentfuncs.TeamCommitmentBar = function(props) {
    var used  = props.data.team_picks_used || 0;
    var total = props.data.team_total      || 1;
    var pct   = Math.min(used / total, 1) * 100;
    var color = props.data.team_color      || '#0071e3';
    var dim   = props.data.Display_Status &&
                (props.data.Display_Status[0] === '✓' || props.data.Display_Status[0] === '❌');

    return React.createElement('div', {
        style: {display:'flex', alignItems:'center', gap:'6px', width:'100%', padding:'0 4px', opacity: dim ? 0.4 : 1}
    },
        React.createElement('div', {
            style: {position:'relative', flex:'1 1 0', minWidth:'20px', height:'8px',
                    background:'#e5e5ea', borderRadius:'4px', overflow:'hidden'}
        },
            React.createElement('div', {
                style: {position:'absolute', left:0, top:0, bottom:0, width: pct + '%',
                        background: color, borderRadius:'4px'}
            })
        ),
        React.createElement('span', {
            style: {fontSize:'10px', color:'#8e8e93', whiteSpace:'nowrap', flexShrink:0,
                    fontVariantNumeric:'tabular-nums'}
        }, used + '/' + total)
    );
};
