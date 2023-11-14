import pandas as pd

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

def simplifyFiles(files, teams):
	
	dfPlayerMaster, dfPlayerRatings, dfPlayerSkaterRS, dfPlayerSkaterPO, dfTeamData, dfTeamLines, dfPlayerContract, dfTeamStats, dfTeamStatsPO, dfTeamRecords = files
	
	dfPlayerMasterSimplified = dfPlayerMaster[dfPlayerMaster['TeamId'].isin([teams])]
	dfPlayerMasterSimplified = dfPlayerMasterSimplified.drop(dfPlayerMasterSimplified.iloc[:, 9:], axis = 1)
	dfPlayerMasterSimplified = dfPlayerMasterSimplified.drop(columns=['FranchiseId'], axis = 1)
	
	dfPlayerSkaterRSSimplified = dfPlayerSkaterRS.drop(columns = ['TeamId', 'FranchiseId'], axis = 1)
	
	dfPlayerSkaterPOSimplified = dfPlayerSkaterPO[dfPlayerSkaterPO['TeamId'].isin([teams])]
	dfPlayerSkaterPOSimplified = dfPlayerSkaterPOSimplified.drop(columns = ['TeamId', 'FranchiseId'], axis = 1)

	dfPlayerContractSimplified = dfPlayerContract.drop(columns=['Team'])

	dfExport = dfPlayerMasterSimplified.merge(dfPlayerRatings, on = 'PlayerId')
	dfExport = dfExport.merge(dfPlayerSkaterRSSimplified, on = 'PlayerId', suffixes = ('oalie', None))	

#	print(dfPlayerSkaterPOSimplified)
	if dfPlayerSkaterPOSimplified.empty == False:
		dfExport = dfExport.merge(dfPlayerSkaterPOSimplified, on = 'PlayerId', suffixes = ('_RS', '_PO'))	

	dfExport = dfExport.merge(dfPlayerContractSimplified, on = 'PlayerId')
	
	dfExport.to_csv('simplifiedCSV/player_master_simplified.csv', index=False)
	print(dfTeamLines)	
	dfTeamLinesSimplified = dfTeamLines[dfTeamLines['TeamId'].isin([teams])]
	dfTeamLinesSimplified = dfTeamLinesSimplified.drop(dfTeamLinesSimplified.iloc[:, 19:92], axis = 1)
	dfTeamLinesSimplified = dfTeamLinesSimplified.drop(columns = ['Extra Attacker 1', 'Extra Attacker 2', 'Unnamed: 96'], axis = 1)	
	
	dfTeamDataSimplified = dfTeamData.drop(dfTeamData.iloc[:, 5:], axis = 1)
	dfTeamDataSimplified = dfTeamDataSimplified.drop(columns=['LeagueId'])
	
	dfTeamStatsPOSimplified = dfTeamStatsPO[dfTeamStatsPO['TeamId'].isin([teams])]
	
	dfTeamRecordsSimplified = dfTeamRecords.drop(columns=['League Id', 'Conf Id', 'Div Id', 'Goals For', 'Goals Against'])
	dfTeamRecordsSimplified = dfTeamRecordsSimplified.rename(columns={'Team Id': 'TeamId'})	

	dfExport2 = dfTeamLinesSimplified.merge(dfTeamDataSimplified, on = 'TeamId')
	
	dfExport2 = dfExport2.merge(dfTeamRecordsSimplified, on = 'TeamId')
	
	dfExport2 = dfExport2.merge(dfTeamStats, on = 'TeamId')
	
	if dfTeamStatsPOSimplified.empty == False:
		dfExport2 = dfExport2.merge(dfTeamStatsPOSimplified, on = 'TeamId', suffixes = ('Team_RS', 'Team_PO'))

	dfTemp2 = pd.DataFrame() 
	dfTemp = pd.DataFrame()
	
#	dfExport = dfExport.add_suffix('1', axis = 1)
	
	for x in range(len(dfExport.index)):
#		dfExport = dfExport.add_suffix(str(x), axis = 1)	
		dfTemp = pd.DataFrame(dfExport.iloc[[x]], columns = dfExport.columns)
		if x != 0:
			dfTemp = dfTemp.add_suffix(str(x), axis = 1)	
			dfTemp = dfTemp.drop(columns = "TeamId" + str(x))
		dfTemp = dfTemp.reset_index()
		dfTemp2 = pd.concat([dfTemp2, dfTemp], axis = 1)
		dfTemp2 = dfTemp2.drop(columns = 'index')
#		dfTemp2 = dfTemp2.reset_index()	
#		print(dfTemp2)		
		dfTemp.columns = dfTemp.columns.str.removesuffix(str(x))
#		dfTemp2 = pd.concat([dfTemp2, dfTemp], axis = 1)
		
	dfExport2 = pd.merge(dfExport2, dfTemp2, on ='TeamId')

#	dfTemp2 = dfTemp2.reset_index()
#	for x in dfExport2.iterrows():
#		print(x)

	dfExport2.to_csv('simplifiedCSV/team_master_simplified.csv', index = False)
	
def main():
	files = importFiles()
	simplifyFiles(files, str(19))

if __name__ == "__main__":
	main()
