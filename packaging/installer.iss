#ifndef MyAppVersion
  #define MyAppVersion "0.0.0"
#endif

#ifndef MyAppExeName
  #define MyAppExeName "VSL.UI.exe"
#endif

#ifndef OutputBaseFilename
  #define OutputBaseFilename "VSL-Setup"
#endif

#ifndef SourceDir
  #define SourceDir "..\\artifacts\\publish\\VSL.UI\\win-x64"
#endif

#ifndef SetupIconFile
  #define SetupIconFile SourceDir + "\\" + MyAppExeName
#endif

#define MyAppId "{{DD35AE14-2A20-483D-97A6-BD649EEDEA61}"
#define MyAppName "Vintage Story Server Launcher (VSL)"
#define MyAppPublisher "VintageStory Community"
#define MyAppURL "https://github.com/VintageStory-Community/VSL"

[Setup]
AppId={#MyAppId}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={localappdata}\Programs\VSL
DefaultGroupName=VSL
UninstallDisplayIcon={app}\{#MyAppExeName}
SetupIconFile={#SetupIconFile}
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
DisableProgramGroupPage=yes
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
OutputDir=.
OutputBaseFilename={#OutputBaseFilename}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"

[Files]
Source: "{#SourceDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\VSL\VSL"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\VSL"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Launch VSL"; Flags: nowait postinstall skipifsilent

[CustomMessages]
english.RemoveUserDataPrompt=Do you want to remove VSL user data (profiles, saves, configs, logs)?%n%nData path: %1

[Code]
procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  DataRoot: string;
begin
  if CurUninstallStep <> usPostUninstall then
  begin
    exit;
  end;

  DataRoot := ExpandConstant('{localappdata}\VSL');
  if not DirExists(DataRoot) then
  begin
    exit;
  end;

  if MsgBox(FmtMessage(CustomMessage('RemoveUserDataPrompt'), [DataRoot]), mbConfirmation, MB_YESNO or MB_DEFBUTTON2) = IDYES then
  begin
    DelTree(DataRoot, True, True, True);
  end;
end;
