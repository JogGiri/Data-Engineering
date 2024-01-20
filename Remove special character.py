def removeSpecialChars(strin):
  if strin is not None:
    if strin != "":
      strin = str(strin)
      if(strin.startswith("(") and strin.endswith(")")):
        strin = "-"+strin
      strin = re.sub('[^a-zA-Z0-9\.-]', '', str(strin))
      if(strin.startswith("-")):
        strin = "-"+strin.replace("-","")
      else:
        strin = strin.replace("-","")
      strArray = strin.split(".")
      if(len(strArray) > 1 ):
        return strin[:strin.find('.')]+"."+strin[strin.find('.'):].replace(".","")
      else:
        return strin
    else:
      return None
  else:
    return None
