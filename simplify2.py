import pandas as pd
import os

# Importing CSV files
def importFiles():
	dfPlayerMaster = pd.read_csv('csv/player_master.csv', sep = ";", on_bad_lines='skip', encoding='ISO-8859-15', dtype=str)
	dfPlayerRatings = pd.read_csv('csv/player_ratings.csv', sep = ";", on_bad_lines='skip', encoding='ISO-8859-15', dtype=str)
	dfPlayerSkaterRS = pd.read_csv('csv/player_skater_stats_rs.csv', sep = ';', encoding='ISO-8859-15', dtype=str)
	dfPlayerSkaterPO = pd.read_csv('csv/player_skater_stats_po.csv', sep = ';', encoding='ISO-8859-15', dtype=str)
	dfTeamData = pd.read_csv('csv/team_data.csv', sep = ';', encoding='ISO-8859-15', dtype=str)
	dfTeamLines = pd.read_csv('csv/team_lines.csv', sep = ';', encoding='ISO-8859-15',dtype=str,index_col=False)
	dfPlayerContract = pd.read_csv('csv/player_contract.csv', sep = ';', encoding='ISO-8859-15', dtype=str)
	dfTeamStats = pd.read_csv('csv/team_stats.csv', sep = ';', encoding='ISO-8859-15', dtype=str)
	dfTeamStatsPO = pd.read_csv('csv/team_stats_playoffs.csv', sep = ';', encoding='ISO-8859-15', dtype=str)
	dfTeamRecords = pd.read_csv('csv/team_records.csv', sep = ';', encoding='ISO-8859-15', dtype=str)

	return([dfPlayerMaster, dfPlayerRatings, dfPlayerSkaterRS, dfPlayerSkaterPO, dfTeamData, dfTeamLines, dfPlayerContract, dfTeamStats, dfTeamStatsPO, dfTeamRecords])

def getLeagues(dfTeamData):
	#get number of leagues to check for
#	print("Number of leagues: ")
	numLeagues = int(1)
	leagues = ["0"]

	#get league ids
#	print("Enter league Ids: ")
#	for x in range(numLeagues):
#		leagues.append(input())

	#get teams from leagues input
	teams = []
	for index, row in dfTeamData.iterrows():
		for x in range(len(leagues)):
			if row[1] == leagues[x]:
				teams.append(row[0])
				
	return(teams, leagues)

def simplifyFiles(files, teams):
	
	dfPlayerMaster, dfPlayerRatings, dfPlayerSkaterRS, dfPlayerSkaterPO, dfTeamData, dfTeamLines, dfPlayerContract, dfTeamStats, dfTeamStatsPO, dfTeamRecords = files
	
	dfPlayerMasterSimplified = dfPlayerMaster[dfPlayerMaster['TeamId'].isin([teams])]
	dfPlayerMasterSimplified = dfPlayerMasterSimplified.drop(dfPlayerMasterSimplified.iloc[:, 9:], axis = 1)
	dfPlayerMasterSimplified = dfPlayerMasterSimplified.drop(columns=['FranchiseId'], axis = 1)
	
	dfPlayerSkaterRSSimplified = dfPlayerSkaterRS.drop(columns = ['TeamId', 'FranchiseId'], axis = 1)
	dfPlayerSkaterRSSimplified = dfPlayerSkaterRSSimplified.add_suffix('_RS')
	dfPlayerSkaterRSSimplified = dfPlayerSkaterRSSimplified.rename(columns={"PlayerId_RS": "PlayerId"})

	dfPlayerSkaterPOSimplified = dfPlayerSkaterPO[dfPlayerSkaterPO['TeamId'].isin([teams])]
	dfPlayerSkaterPOSimplified = dfPlayerSkaterPOSimplified.drop(columns = ['TeamId', 'FranchiseId'], axis = 1)
	dfPlayerSkaterPOSimplified = dfPlayerSkaterPOSimplified.add_suffix('_PO')
	dfPlayerSkaterPOSimplified = dfPlayerSkaterPOSimplified.rename(columns={"PlayerId_PO": "PlayerId"})

	dfPlayerContractSimplified = dfPlayerContract.drop(columns=['Team'])
	dfPlayerRatings = dfPlayerRatings.rename(columns={"G": "Goalie"})

	dfExport = dfPlayerMasterSimplified.merge(dfPlayerRatings, on = 'PlayerId')
	dfExport = dfExport.merge(dfPlayerSkaterRSSimplified, on = 'PlayerId')	

	if dfPlayerSkaterPOSimplified.empty == False:
		dfExport = dfExport.merge(dfPlayerSkaterPOSimplified, on = 'PlayerId', suffixes = ('_RS', '_PO'))
	else:
		dfExport = pd.concat([dfExport, dfPlayerSkaterPOSimplified])


	dfExport = dfExport.merge(dfPlayerContractSimplified, on = 'PlayerId')
	dfTeamLinesSimplified = dfTeamLines[dfTeamLines['TeamId'].isin([teams])]
	dfTeamLinesSimplified = dfTeamLinesSimplified.drop(dfTeamLinesSimplified.iloc[:, 19:92], axis = 1)
	dfTeamLinesSimplified = dfTeamLinesSimplified.drop(columns = ['Extra Attacker 1', 'Extra Attacker 2', 'Unnamed: 96'], axis = 1)	
	
	list1 = dfTeamLinesSimplified.values.tolist()
	list2 = dfPlayerMasterSimplified.values.tolist()

	for x in range(len(list1)):
		for z in range(1, len(list1[x])):
			for a in range(len(list2)):
				if list1[x][z] == list2[a][0]:
					list1[x][z] = list2[a][3]

	#export back to dataframe and re label columns				
	dfTeamLinesSimplified1 = pd.DataFrame(list1)
	dfTeamLinesSimplified1 = dfTeamLinesSimplified1.rename(columns={0: "TeamId", 1: "LW1", 2: "C1", 3: "RW1", 4: "LD1", 5: "RD1", 6: "LW2", 7: "C2", 8: "RW2", 9: "LD2", 10: "RD2", 11: "LW3", 12: "C3", 13: "RW3", 14: "LD3", 15: "RD3", 16: "LW4", 17: "C4", 18: "RW4", 19: "G1", 20: "G2"})
#	dfTeamLinesSimplified = dfTeamLinesSimplified.drop(21, axis = 1)
	dfTeamDataSimplified = dfTeamData.drop(dfTeamData.iloc[:, 5:13], axis = 1)
	dfTeamDataSimplified = dfTeamDataSimplified.drop(dfTeamDataSimplified.iloc[:, 7:], axis = 1)
	dfTeamDataSimplified = dfTeamDataSimplified.drop(columns=['LeagueId'])
	
	dfTeamStats = dfTeamStats.add_suffix('_RS')
	dfTeamStats = dfTeamStats.rename(columns={'TeamId_RS': 'TeamId'})
	dfTeamStatsPOSimplified = dfTeamStatsPO[dfTeamStatsPO['TeamId'].isin([teams])]
	dfTeamStatsPOSimplified = dfTeamStatsPOSimplified.add_suffix('_PO')
	dfTeamStatsPOSimplified = dfTeamStatsPOSimplified.rename(columns={'TeamId_PO': 'TeamId'})

	dfTeamRecordsSimplified = dfTeamRecords.drop(columns=['League Id', 'Conf Id', 'Div Id', 'Goals For', 'Goals Against'])
	dfTeamRecordsSimplified = dfTeamRecordsSimplified.rename(columns={'Team Id': 'TeamId'})	

	dfExport2 = dfTeamLinesSimplified1.merge(dfTeamDataSimplified, on = 'TeamId')

	dfExport2 = dfExport2.merge(dfTeamRecordsSimplified, on = 'TeamId')
	dfExport2 = dfExport2.merge(dfTeamStats, on = 'TeamId')

	if dfTeamStatsPOSimplified.empty == False:
	    dfExport2 = dfExport2.merge(dfTeamStatsPOSimplified, on = 'TeamId', suffixes = ('Team_RS', 'Team_PO'))
	else:
		dfExport2 = pd.concat([dfExport2, dfTeamStatsPOSimplified])

	dfTemp2 = pd.DataFrame()
	dfTemp = pd.DataFrame()
	
	for x in range(len(dfExport.index)):	
		dfTemp = pd.DataFrame(dfExport.iloc[[x]], columns = dfExport.columns)
		dfTemp = dfTemp.add_suffix(str(x), axis = 1)	
#		dfTemp = dfTemp.drop(columns = "TeamId" + str(x))
		if x == 0:
			dfTemp = dfTemp.rename(columns={'TeamId0': 'TeamId'})
		dfTemp = dfTemp.reset_index()
		dfTemp2 = pd.concat([dfTemp2, dfTemp], axis = 1)
		dfTemp2 = dfTemp2.drop(columns = 'index')		
		dfTemp.columns = dfTemp.columns.str.removesuffix(str(x))

	print(dfTemp2)
		
	dfExport2 = pd.merge(dfExport2, dfTemp2, on ='TeamId', suffixes = ('_line', None))

	dfExport2.to_csv('simplifiedCSV/league_master_simplified.csv', index = False, mode = 'a', header=not os.path.exists('simplifiedCSV/league_master_simplified.csv'))
	
def main():
	files = importFiles()
	teams, leagues = getLeagues(files[4])
	for x in range(len(teams)):
		simplifyFiles(files, str(teams[x]))

if __name__ == "__main__":
	main()
