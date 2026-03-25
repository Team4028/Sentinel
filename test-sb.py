import statbotics

sb = statbotics.Statbotics()
print({s['team']: s['epa']['total_points']['mean'] for s in sb.get_team_events(event='2026paca', limit=1000, fields=["team", "epa"])})